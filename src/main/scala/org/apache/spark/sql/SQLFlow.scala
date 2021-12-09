/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.io.File
import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

import org.apache.commons.io.FileUtils;

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, LeftExistence}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation

abstract class BaseSQLFlow extends PredicateHelper with Logging {

  private val cachedNodeColor = "lightblue"

  private val nextNodeId = new AtomicInteger(0)

  def collectEdges(
    tempView: String,
    plan: LogicalPlan,
    nodeMap: mutable.Map[String, String]): Seq[String]

  def catalogToSQLFlow(session: SparkSession): String = {
    val nodeMap = mutable.Map[String, String]()

    val catalog = session.sessionState.catalog
    val views = catalog.listDatabases().flatMap { db =>
      catalog.listTables(db).flatMap { ident =>
        try {
          val tableMeta = catalog.getTableMetadata(ident)
          tableMeta.viewText.map { viewText =>
            val viewName = tableMeta.identifier.unquotedString
            val parsed = session.sessionState.sqlParser.parsePlan(viewText)
            val analyzed = session.sessionState.analyzer.execute(parsed)
            (viewName, analyzed)
          }
        } catch {
          case _: NoSuchTableException =>
            None
        }
      }
    }
    val tempViews = catalog.getTempViewNames().map { viewName =>
      val analyzed = session.sessionState.analyzer.execute(catalog.getTempView(viewName).get)
      (viewName, analyzed)
    }

    // Generate node labels for views if necessary
    (tempViews ++ views).foreach { case (viewName, analyzed) =>
      nodeMap(viewName) = generateTableNodeString(analyzed, viewName, isCached(analyzed))
    }

    val subplanToTempViewMap = tempViews.map { case (viewName, analyzed) =>
      (analyzed.semanticHash(), viewName)
    }.toMap

    def skipToReplaceWithTempView(p: LogicalPlan): Boolean = p match {
      case _: View => true
      case _ => false
    }

    def blacklistToReplaceSubplan(p: LogicalPlan): Boolean = p match {
      case Project(_, sp) if blacklistToReplaceSubplan(sp) => true
      case SubqueryAlias(_, _: LocalRelation | _: OneRowRelation) => true
      case _ => false
    }

    val edges = (views ++ tempViews).map { case (viewName, analyzed) =>
      def replaceWithTempViewNodeInSubqueries(expr: Expression): Expression = {
        expr.transformDown {
          case sq @ ScalarSubquery(sp, _, _) =>
            sq.copy(plan = sp.transformDown {
              case p => replaceWithTempViewNode(p)
            })

          case sq @ Exists(sp, _, _) =>
            sq.copy(plan = sp.transformDown {
              case p => replaceWithTempViewNode(p)
            })

          case sq @ InSubquery(_, q @ ListQuery(sp, _, _, _)) =>
            sq.copy(query = q.copy(plan = sp.transformDown {
              case p => replaceWithTempViewNode(p)
            }))
        }
      }
      def replaceWithTempViewNode(p: LogicalPlan): LogicalPlan = p match {
        case p if skipToReplaceWithTempView(p) => p

        case s @ SubqueryAlias(AliasIdentifier(name, _), v: View) =>
          if (!v.isTempView) {
            ViewNode(v.desc.identifier.unquotedString, s.output)
          } else {
            TempViewNode(name, s.output)
          }

        case s @ SubqueryAlias(AliasIdentifier(name, Nil),
            Project(_, _: LocalRelation | _: OneRowRelation)) =>
          TempViewNode(getNodeNameWithId(name), s.output)

        case s @ SubqueryAlias(AliasIdentifier(name, Nil), _: LocalRelation) =>
          TempViewNode(getNodeNameWithId(name), s.output)

        case p if subplanToTempViewMap.contains(p.semanticHash()) &&
            viewName != subplanToTempViewMap(p.semanticHash()) &&
            !blacklistToReplaceSubplan(p) =>
          val v = subplanToTempViewMap(p.semanticHash())
          TempViewNode(v, p.output)

        case Project(projList, child) if projList.exists(SubqueryExpression.hasSubquery) =>
          val newProjList = projList.map(replaceWithTempViewNodeInSubqueries)
            .asInstanceOf[Seq[NamedExpression]]
          Project(newProjList, child)

        case Aggregate(groupingExprs, aggExprs, child)
            if aggExprs.exists(SubqueryExpression.hasSubquery) =>
          val newAggExprs = aggExprs.map(replaceWithTempViewNodeInSubqueries)
            .asInstanceOf[Seq[NamedExpression]]
          Aggregate(groupingExprs, newAggExprs, child)

        case Filter(cond, child) if SubqueryExpression.hasSubquery(cond) =>
          Filter(replaceWithTempViewNodeInSubqueries(cond), child)

        case p => p // Do nothing
      }

      val optimized = session.sessionState.optimizer.execute(analyzed.transformUp {
        case p if isCached(p) => CachedNode(p)
      }.transformDown {
        case p => replaceWithTempViewNode(p)
      })

      if (!optimized.isInstanceOf[ViewNode] || !optimized.isInstanceOf[TempViewNode]) {
        collectEdges(viewName, optimized, nodeMap)
      } else {
        // If a given plan is `TempView t1, [a#102, b#103]`, `nodeName` should be equal to
        // `tempView` and we don't need a new node and edges for `TempView`.
        Nil
      }
    }

    generateGraphString(nodeMap.values.toSeq, edges.flatten)
  }

  private def isCached(plan: LogicalPlan): Boolean = {
    val session = SparkSession.getActiveSession.getOrElse {
      throw new IllegalStateException("Active SparkSession not found")
    }
    session.sharedState.cacheManager.lookupCachedData(plan).isDefined
  }

  private def getNodeNameWithId(name: String): String = {
    s"${name}_${nextNodeId.getAndIncrement()}"
  }

  protected def getNodeName(p: LogicalPlan) = p match {
    case ViewNode(name, _) => name
    case TempViewNode(name, _) => name
    case LogicalRelation(_, _, Some(table), false) => table.qualifiedName
    case HiveTableRelation(table, _, _, _, _) => table.qualifiedName
    case j: Join => getNodeNameWithId(s"${p.nodeName}_${j.joinType}")
    case _ => getNodeNameWithId(p.nodeName)
  }

  private def normalizeForHtml(str: String) = {
    str.replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
  }

  protected def generateGraphString(nodes: Seq[String], edges: Seq[String]) = {
    if (nodes.nonEmpty) {
      s"""
         |digraph {
         |  graph [pad="0.5" nodesep="0.5" ranksep="1" fontname="Helvetica" rankdir=LR];
         |  node [shape=plaintext]
         |
         |  ${nodes.sorted.mkString("\n")}
         |  ${edges.sorted.mkString("\n")}
         |}
       """.stripMargin
    } else {
      ""
    }
  }

  private def generateTableNodeString(
      p: LogicalPlan,
      nodeName: String,
      isCached: Boolean = false,
      force: Boolean = false) = {
    if (force || p.output.nonEmpty) {
      val nodeColor = if (isCached) cachedNodeColor else "black"
      val outputAttrs = p.output.zipWithIndex.map { case (attr, i) =>
        s"""<tr><td port="$i">${normalizeForHtml(attr.name)}</td></tr>"""
      }
      // scalastyle:off line.size.limit
      s"""
         |"$nodeName" [color="$nodeColor" label=<
         |<table>
         |  <tr><td bgcolor="$nodeColor" port="nodeName"><i><font color="white">$nodeName</font></i></td></tr>
         |  ${outputAttrs.mkString("\n")}
         |</table>>];
       """.stripMargin
      // scalastyle:on line.size.limit
    } else {
      ""
    }
  }

  private def generatePlanNodeString(
      p: LogicalPlan,
      nodeName: String,
      isCached: Boolean = false,
      force: Boolean = false) = {
    if (force || p.output.nonEmpty) {
      val nodeColor = if (isCached) cachedNodeColor else "lightgray"
      val outputAttrs = p.output.zipWithIndex.map { case (attr, i) =>
        s"""<tr><td port="$i">${normalizeForHtml(attr.name)}</td></tr>"""
      }
      // scalastyle:off line.size.limit
      s"""
         |"$nodeName" [label=<
         |<table color="$nodeColor" border="1" cellborder="0" cellspacing="0">
         |  <tr><td bgcolor="$nodeColor" port="nodeName"><i>$nodeName</i></td></tr>
         |  ${outputAttrs.mkString("\n")}
         |</table>>];
       """.stripMargin
      // scalastyle:on line.size.limit
    } else {
      ""
    }
  }

  protected def generateNodeString(
      p: LogicalPlan,
      nodeName: String,
      isCached: Boolean,
      force: Boolean = false): String = {
    p match {
      case _: View | _: ViewNode |_: TempViewNode | _: LocalRelation | _: LogicalRelation |
           _: InMemoryRelation | _: HiveTableRelation =>
        generateTableNodeString(p, nodeName, isCached, force)
      case _ =>
        generatePlanNodeString(p, nodeName, isCached, force)
    }
  }

  protected def generateNodeString(p: LogicalPlan, nodeName: String): String = {
    generateNodeString(p, nodeName, isCached(p))
  }
}

case class SQLFlow() extends BaseSQLFlow {

  override def collectEdges(
      tempView: String,
      plan: LogicalPlan,
      nodeMap: mutable.Map[String, String]): Seq[String] = {
    val (inputNodeId, edges) = traversePlanRecursively(plan, nodeMap, isRoot = true)
    val edgesToTempView = if (inputNodeId != tempView) {
      plan.output.indices.map { i =>
        s""""$inputNodeId":$i -> "$tempView":$i;"""
      }
    } else {
      Nil
    }
    edges ++ edgesToTempView
  }

  def planToSQLFlow(plan: LogicalPlan): String = {
    val nodeMap = mutable.Map[String, String]()
    val (_, edges) = traversePlanRecursively(plan, nodeMap)
    generateGraphString(nodeMap.values.toSeq, edges)
  }

  private def collectEdgesInPlan(
      plan: LogicalPlan,
      curNodeName: String,
      inputNodeIds: Seq[String]): Seq[String] = {
    val inputNodeIdsWithOutput = inputNodeIds.zip(plan.children.map(_.output))
    val inputAttrSeq = inputNodeIdsWithOutput.map { case (nodeId, output) =>
      output.zipWithIndex.map { case (a, i) =>
        a -> s""""$nodeId":$i"""
      }
    }
    val inputAttrMap = AttributeMap(inputAttrSeq.flatten)
    val outputAttrWithIndex = plan.output.zipWithIndex
    val edges = plan match {
      case Aggregate(_, aggExprs, _) =>
        aggExprs.zip(outputAttrWithIndex).flatMap { case (ne, (_, i)) =>
          ne.references.filter(inputAttrMap.contains).map { attr =>
            s"""${inputAttrMap(attr)} -> "$curNodeName":$i;"""
          }
        }

      case Project(projList, _) =>
        projList.zip(outputAttrWithIndex).flatMap { case (ne, (_, i)) =>
          ne.references.filter(inputAttrMap.contains).map { attr =>
            s"""${inputAttrMap(attr)} -> "$curNodeName":$i;"""
          }
        }

      case g @ Generate(generator, _, _, _, generatorOutput, _) =>
        val edgesForChildren = g.requiredChildOutput.zipWithIndex.flatMap { case (attr, i) =>
          inputAttrMap.get(attr).map { input => s"""$input -> "$curNodeName":$i;"""}
        }
        val edgeForGenerator = generator.references.flatMap(inputAttrMap.get).headOption
          .map { genInput =>
            generatorOutput.zipWithIndex.map { case (attr, i) =>
              s"""$genInput -> "$curNodeName":${g.requiredChildOutput.size + i}"""
            }
          }
        edgesForChildren ++ edgeForGenerator.seq.flatten

      case Expand(projections, _, _) =>
        projections.transpose.zipWithIndex.flatMap { case (projs, i) =>
          projs.flatMap(e => e.references.flatMap(inputAttrMap.get))
            .map { input => s"""$input -> "$curNodeName":$i;"""}
            .distinct
        }

      case _: Union =>
        inputAttrSeq.transpose.zipWithIndex.flatMap { case (attrs, i) =>
          attrs.map { case (_, input) => s"""$input -> "$curNodeName":$i"""}
        }

      case Join(_, _, joinType, condition, _) =>
        // To avoid ambiguous joins, we need this
        val Seq(left, right) = inputAttrSeq
        joinType match {
          case LeftExistence(_) =>
            val leftAttrSet = AttributeSet(left.map(_._1))
            val leftAttrIndexMap = AttributeMap(left.map(_._1).zipWithIndex)
            val predicateEdges = condition.map { c =>
              val referenceSeq = splitConjunctivePredicates(c).map(_.references)
              right.flatMap { case (attr, input) =>
                val leftAttrs = referenceSeq.flatMap { refs =>
                  if (refs.contains(attr)) {
                    refs.intersect(leftAttrSet).toSeq
                  } else {
                    Nil
                  }
                }
                leftAttrs.map { attr =>
                  s"""$input -> "$curNodeName":${leftAttrIndexMap(attr)};"""
                }
              }
            }
            val joinOutputEdges = left.map(_._2).zipWithIndex.map { case (input, i) =>
              s"""$input -> "$curNodeName":$i;"""
            }
            joinOutputEdges ++ predicateEdges.getOrElse(Nil)
          case _ =>
            (left ++ right).map(_._2).zipWithIndex.map { case (input, i) =>
              s"""$input -> "$curNodeName":$i;"""
            }
        }

      // TODO: Needs to check if the other Python-related plan nodes are handled correctly
      case _: FlatMapGroupsInPandas =>
        inputAttrSeq.head.zip(outputAttrWithIndex).map { case ((_, input), (_, i)) =>
          s"""$input -> "$curNodeName":$i;"""
        }

      case _ =>
        outputAttrWithIndex.flatMap { case (attr, i) =>
          inputAttrMap.get(attr).map { input => s"""$input -> "$curNodeName":$i;"""}
        }
    }

    if (edges.isEmpty) {
      inputNodeIdsWithOutput.flatMap { case (inputNodeId, output) =>
        if (output.isEmpty) {
          s""""$inputNodeId":nodeName -> "$curNodeName":nodeName""" :: Nil
        } else {
          output.zipWithIndex.map { case (_, i) =>
            s""""$inputNodeId":$i -> "$curNodeName":nodeName"""
          }
        }
      }
    } else {
      edges
    }
  }

  object SubqueryPredicate {
    def unapply(cond: Expression): Option[Seq[(ScalarSubquery, Seq[Attribute])]] = {
      val comps = cond.collect {
        case BinaryComparison(le, re) if SubqueryExpression.hasSubquery(re) =>
          val ss = re.collectFirst { case ss: ScalarSubquery => ss }.get
          (ss, le.references.toSeq)
      }
      if (comps.nonEmpty) {
        Some(comps)
      } else {
        None
      }
    }
  }

  private def collectEdgesInSubqueries(
      nodeName: String,
      plan: LogicalPlan,
      nodeMap: mutable.Map[String, String]): Seq[String] = {
    val hasSbuqueres = plan.expressions.exists(SubqueryExpression.hasSubquery)
    if (hasSbuqueres) {
      val planOutputMap = AttributeMap(plan.output.zipWithIndex)

      def collectEdgesInExprs(ne: NamedExpression): Seq[String] = {
        val attr = ne.toAttribute
        val subquries = ne.collect { case ss: ScalarSubquery => ss }
        subquries.flatMap { ss =>
          val (inputNodeId, edges) = traversePlanRecursively(ss.plan, nodeMap)
          edges ++ ss.plan.output.indices.map { i =>
            if (planOutputMap.contains(attr)) {
              s""""$inputNodeId":$i -> "$nodeName":${planOutputMap(attr)}"""
            } else {
              s""""$inputNodeId":$i -> "$nodeName":nodeName"""
            }
          }
        }
      }

      plan match {
        case Filter(SubqueryPredicate(subqueries), _) =>
          subqueries.flatMap { case (ss, attrs) =>
            val (inputNodeId, edges) = traversePlanRecursively(ss.plan, nodeMap)
            edges ++ ss.plan.output.indices.flatMap { i =>
              val edgesInSubqueries = attrs.flatMap { attr =>
                if (planOutputMap.contains(attr)) {
                  Some(s""""$inputNodeId":$i -> "$nodeName":${planOutputMap(attr)}""")
                } else {
                  None
                }
              }

              if (edgesInSubqueries.nonEmpty) {
                edgesInSubqueries
              } else {
                s""""$inputNodeId":$i -> "$nodeName":nodeName""" :: Nil
              }
            }
          }

        case Project(projList, _) =>
          projList.filter(SubqueryExpression.hasSubquery).flatMap { ne =>
            collectEdgesInExprs(ne)
          }

        case Aggregate(_, aggregateExprs, _) =>
          aggregateExprs.filter(SubqueryExpression.hasSubquery).flatMap { ne =>
            collectEdgesInExprs(ne)
          }

        case _ => // fallback case
          val subqueries = plan.expressions.flatMap(_.collect { case ss: ScalarSubquery => ss })
          subqueries.flatMap { ss =>
            val (inputNodeId, edges) = traversePlanRecursively(ss.plan, nodeMap)
            edges ++ ss.plan.output.indices.map { i =>
              s""""$inputNodeId":$i -> "$nodeName":nodeName"""
            }
          }
      }
    } else {
      Nil
    }
  }

  private def tryCreateNode(
      plan: LogicalPlan,
      nodeMap: mutable.Map[String, String],
      cached: Boolean = false,
      force: Boolean = false): String = {
    val nodeName = getNodeName(plan)
    if (force || plan.output.nonEmpty) {
      // Generate a node label for a plan if necessary
      nodeMap.getOrElseUpdate(nodeName, generateNodeString(plan, nodeName, cached, force))
    }
    nodeName
  }

  private def traversePlanRecursively(
    plan: LogicalPlan,
    nodeMap: mutable.Map[String, String],
    cached: Boolean = false,
    isRoot: Boolean = false): (String, Seq[String]) = plan match {
    case _: LeafNode =>
      val nodeName = tryCreateNode(plan, nodeMap)
      (nodeName, Nil)

    case CachedNode(cachedPlan) =>
      traversePlanRecursively(cachedPlan, nodeMap, cached = !isRoot)

    case _ =>
      val edgesInChildren = plan.children.map(traversePlanRecursively(_, nodeMap))
      if (plan.output.nonEmpty) {
        val nodeName = tryCreateNode(plan, nodeMap, cached)
        val edges = collectEdgesInPlan(plan, nodeName, edgesInChildren.map(_._1))
        val edgesInSubqueries = collectEdgesInSubqueries(nodeName, plan, nodeMap)
        (nodeName, edges ++ edgesInChildren.flatMap(_._2) ++ edgesInSubqueries)
      } else {
        val nodeName = tryCreateNode(plan, nodeMap, force = true)
        val edges = edgesInChildren.map(_._1).zip(plan.children.map(_.output))
          .flatMap { case (inputNodeId, output) =>
            output.zipWithIndex.map { case (_, i) =>
              s""""$inputNodeId":$i -> "$nodeName":nodeName"""
            }
          }
        (nodeName, edges ++ edgesInChildren.flatMap(_._2))
      }
  }
}

case class SQLContractedFlow() extends BaseSQLFlow {

  override def collectEdges(
      tempView: String,
      plan: LogicalPlan,
      nodeMap: mutable.Map[String, String]): Seq[String] = {
    val outputAttrMap = plan.output.map(_.exprId).zipWithIndex.toMap
    val (edges, candidateEdges, refMap) = traversePlanRecursively(tempView, plan, nodeMap)
    edges ++ candidateEdges.flatMap { case ((inputNodeId, input), candidates) =>
      val edges = candidates.flatMap { case (input, exprId) =>
        if (inputNodeId != tempView && outputAttrMap.contains(exprId)) {
          Some(s"""$input -> "$tempView":${outputAttrMap(exprId)}""")
        } else {
          None
        }
      }
      if (edges.isEmpty) {
        // TODO: Makes it more precise
        input.zipWithIndex.filter { i => refMap.contains(i._1.exprId) }.map { case (_, i) =>
          s""""$inputNodeId":$i -> "$tempView":nodeName"""
        }
      } else {
        edges
      }
    }
  }

  private def collectRefsRecursively(
      plan: LogicalPlan,
      refMap: mutable.Map[ExprId, mutable.Set[ExprId]]): Unit = {

    object JoinWithCondition {
      def unapply(plan: LogicalPlan): Option[Seq[(Seq[Attribute], Seq[Attribute])]] = plan match {
        case Join(left, _, _, Some(cond), _) =>
          val comps = cond.collect { case BinaryComparison(e1, e2) =>
            val (leftRefs, rightRefs) = {
              (e1.references ++ e2.references).partition(left.outputSet.contains)
            }
            (leftRefs.toSeq, rightRefs.toSeq)
          }.filter { case (l, r) =>
            l.nonEmpty && r.nonEmpty
          }
          if (comps.nonEmpty) {
            Some(comps)
          } else {
            None
          }
        case _ =>
          None
      }
    }
    plan match {
      case _: LeafNode =>
      case _ =>
        plan.children.foreach(collectRefsRecursively(_, refMap))
        if (plan.output.nonEmpty) {
          def addRefsToMap(k: Attribute, v: Seq[Attribute]): Unit = {
            val refs = refMap.getOrElseUpdate(k.exprId, mutable.Set[ExprId]())
            refs ++= v.map(_.exprId)
          }
          plan match {
            case a @ Aggregate(_, aggExprs, _) =>
              aggExprs.zip(a.output).foreach { case (ae, outputAttr) =>
                ae.references.foreach(addRefsToMap(_, Seq(outputAttr)))
              }

            case p @ Project(projList, _) =>
              projList.zip(p.output).foreach { case (ae, outputAttr) =>
                ae.references.foreach(addRefsToMap(_, Seq(outputAttr)))
              }

            case Generate(generator, _, _, _, generatorOutput, _) =>
              generator.references.foreach { a =>
                generatorOutput.foreach { outputAttr => addRefsToMap(a, Seq(outputAttr)) }
              }

            case e @ Expand(projections, _, _) =>
              projections.transpose.zip(e.output).foreach { case (projs, outputAttr) =>
                projs.flatMap(_.references).foreach(addRefsToMap(_, Seq(outputAttr)))
              }

            case u: Union =>
              u.children.map(_.output).transpose.zip(u.output).foreach { case (attrs, outputAttr) =>
                attrs.foreach(addRefsToMap(_, Seq(outputAttr)))
              }

            case JoinWithCondition(preds) =>
              preds.foreach { case (leftRefs, rightRefs) =>
                leftRefs.foreach { a1 => rightRefs.foreach { a2 =>
                  addRefsToMap(a1, Seq(a1, a2))
                  addRefsToMap(a2, Seq(a1, a2))
                }}
              }

            case Join(left, right, _, _, _) =>
              left.output.foreach { a1 => right.output.foreach { a2 =>
                addRefsToMap(a1, Seq(a1, a2))
                addRefsToMap(a2, Seq(a1, a2))
              }}

            case _ =>
          }
        }

        val inputAttrs = plan.children.flatMap(_.output)
        val candidateOutputRefs = (inputAttrs.map(_.exprId) ++ inputAttrs.flatMap { a =>
          refMap.get(a.exprId)
        }.flatten).toSet
        val planOutput = plan match {
          case Project(projList, _) =>
            projList.flatMap { ne =>
              if (ne.references.nonEmpty) Some(ne.toAttribute) else None
            }

          case Aggregate(_, aggregateExprs, _) =>
            aggregateExprs.flatMap { ne =>
              if (ne.references.nonEmpty) Some(ne.toAttribute) else None
            }

          case Join(left, _, _: ExistenceJoin, _, _) =>
            left.output

          case _ =>
            plan.output
        }
        val missingRefs = (AttributeSet(planOutput) -- plan.producedAttributes).filterNot { a =>
          candidateOutputRefs.contains(a.exprId)
        }
        if (missingRefs.nonEmpty) {
          val msg = s"""
               |refMap does not have enough entries for ${plan.nodeName}:
               |missingRefs: ${missingRefs.mkString(", ")}
               |${plan.treeString}
             """.stripMargin
          if (SQLFlow.isTesting) {
            throw new IllegalStateException(msg)
          } else {
            logWarning(msg)
          }
        }
    }
  }

  private def collectEdgesInSubqueries(
      tempView: String,
      plan: LogicalPlan,
      nodeMap: mutable.Map[String, String]): Seq[String] = {
    val subqueries = plan.collect { case p =>
      p.expressions.flatMap(_.collect { case ss: ScalarSubquery => ss })
    }.flatten
    if (subqueries.nonEmpty) {
      // TODO: Needs to handle Project/Aggregate/Filter nodes
      subqueries.flatMap { ss =>
        val outputAttrSet = ss.plan.output.map(_.exprId).toSet
        val (edges, candidateEdges, refMap) = traversePlanRecursively(tempView, ss.plan, nodeMap)
        edges ++ candidateEdges.flatMap { case ((inputNodeId, input), candidates) =>
          val edges = candidates.flatMap { case (input, exprId) =>
            if (outputAttrSet.contains(exprId)) {
              Some(s"""$input -> "$tempView":nodeName""")
            } else {
              None
            }
          }
          if (edges.isEmpty) {
            // TODO: Makes it more precise
            input.zipWithIndex.filter { i => refMap.contains(i._1.exprId) }.map { case (_, i) =>
              s""""$inputNodeId":$i -> "$tempView":nodeName"""
            }
          } else {
            edges
          }
        }
      }
    } else {
      Nil
    }
  }

  private def traversePlanRecursively(
      tempView: String,
      plan: LogicalPlan,
      nodeMap: mutable.Map[String, String])
    : (Seq[String], Seq[((String, Seq[Attribute]), Seq[(String, ExprId)])],
      Map[ExprId, Set[ExprId]]) = {
    // Collect input nodes
    val inputNodes = plan.collectLeaves().map { p =>
      val nodeName = getNodeName(p)
      nodeMap.getOrElseUpdate(nodeName, generateNodeString(p, nodeName))
      (nodeName, p.output)
    }.groupBy(_._1).map { case (_, v) =>
      v.head
    }

    // Collect references between input/output
    val refMap = mutable.Map[ExprId, mutable.Set[ExprId]]()
    collectRefsRecursively(plan, refMap)

    def traverseInRefMap(
        exprId: ExprId,
        depth: Int = 0,
        seen: Set[ExprId] = Set.empty[ExprId]): Seq[ExprId] = {
      if (seen.contains(exprId) || depth > 128) {
        // Cyclic case
        Nil
      } else {
        refMap.get(exprId).map { exprIds =>
          exprIds.flatMap { id =>
            if (exprId != id) {
              traverseInRefMap(id, depth + 1, seen + exprId)
            } else {
              id :: Nil
            }
          }
        }.map(_.toSeq).getOrElse(exprId :: Nil)
      }
    }

    val candidateEdges = inputNodes.toSeq.map { case (inputNodeId, input) =>
      (inputNodeId, input) -> input.zipWithIndex.flatMap { case (a, i) =>
        traverseInRefMap(a.exprId).map { exprId =>
          (s""""$inputNodeId":$i""", exprId)
        }
      }
    }

    // Handles subqueries if necessary
    val edgesInSubqueries = collectEdgesInSubqueries(tempView, plan, nodeMap)

    (edgesInSubqueries, candidateEdges, refMap.mapValues(_.toSet).toMap)
  }
}

case class CachedNode(cachedPlan: LogicalPlan) extends UnaryNode {
  override lazy val resolved: Boolean = true
  override def output: Seq[Attribute] = cachedPlan.output
  override def child: LogicalPlan = cachedPlan
}

case class ViewNode(name: String, output: Seq[Attribute]) extends LeafNode {
  override lazy val resolved: Boolean = true
}

case class TempViewNode(name: String, output: Seq[Attribute]) extends LeafNode {
  override lazy val resolved: Boolean = true
}

case class SQLFlowHolder[T] private[sql](private val ds: Dataset[T]) {
  import SQLFlow._

  def debugPrintAsSQLFlow(): Unit = {
    // scalastyle:off println
    println(SQLFlow().planToSQLFlow(ds.queryExecution.optimizedPlan))
    // scalastyle:on println
  }

  def saveAsSQLFlow(
      outputDirPath: String,
      filenamePrefix: String = "sqlflow",
      format: String = "svg",
      overwrite: Boolean = false): Unit = {
    val flowString = SQLFlow().planToSQLFlow(ds.queryExecution.optimizedPlan)
    writeSQLFlow(outputDirPath, filenamePrefix, format, flowString, overwrite)
  }
}

object SQLFlow extends Logging {

  val validImageFormatSet = Set("png", "svg")

  implicit def DatasetToSQLFlowHolder[T](ds: Dataset[T]): SQLFlowHolder[T] = {
    new SQLFlowHolder[T](ds)
  }

  private def isCommandAvailable(command: String): Boolean = {
    val attempt = {
      Try(Process(Seq("sh", "-c", s"command -v $command")).run(ProcessLogger(_ => ())).exitValue())
    }
    attempt.isSuccess && attempt.get == 0
  }

  // If the Graphviz dot command installed, converts the generated dot file
  // into a specified-formatted image.
  private def tryGenerateImageFile(format: String, src: String, dst: String): Unit = {
    if (isCommandAvailable("dot")) {
      try {
        val commands = Seq("bash", "-c", s"dot -T$format $src > $dst")
        BlockingLineStream(commands)
      } catch {
        case _ => // Do nothing
      }
    }
  }

  def writeSQLFlow(
      outputDirPath: String,
      filenamePrefix: String,
      format: String,
      flowString: String,
      overwrite: Boolean = false): Unit = {
    if (!SQLFlow.validImageFormatSet.contains(format.toLowerCase(Locale.ROOT))) {
      throw new AnalysisException(s"Invalid image format: $format")
    }
    val outputDir = new File(outputDirPath)
    if (overwrite) {
      FileUtils.deleteDirectory(outputDir)
    }
    if (!outputDir.mkdir()) {
      throw new AnalysisException(if (overwrite) {
        s"`overwrite` is set to true, but could not remove output dir path '$outputDirPath'"
      } else {
        s"output dir path '$outputDirPath' already exists"
      })
    }
    val dotFile = stringToFile(new File(outputDir, s"$filenamePrefix.dot"), flowString)
    val srcFile = dotFile.getAbsolutePath
    val dstFile = new File(outputDir, s"$filenamePrefix.$format").getAbsolutePath
    tryGenerateImageFile(format, srcFile, dstFile)
  }

  def saveAsSQLFlow(
      outputDirPath: String,
      filenamePrefix: String = "sqlflow",
      format: String = "svg",
      contracted: Boolean = false,
      overwrite: Boolean = false): Unit = {
    SparkSession.getActiveSession.map { session =>
      val flowString = if (contracted) {
        SQLContractedFlow().catalogToSQLFlow(session)
      } else {
        SQLFlow().catalogToSQLFlow(session)
      }
      SQLFlow.writeSQLFlow(outputDirPath, filenamePrefix, format, flowString, overwrite)
    }.getOrElse {
      logWarning(s"Active SparkSession not found")
    }
  }

  def toSQLFlowString(contracted: Boolean = false): String = {
    SparkSession.getActiveSession.map { session =>
      if (contracted) {
        SQLContractedFlow().catalogToSQLFlow(session)
      } else {
        SQLFlow().catalogToSQLFlow(session)
      }
    }.getOrElse {
      logWarning("Active SparkSession not found")
      ""
    }
  }

  // Indicates whether Spark is currently running unit tests
  private[sql] lazy val isTesting: Boolean = {
    System.getenv("SPARK_TESTING") != null || System.getProperty("spark.testing") != null
  }

  def debugPrintAsSQLFlow(contracted: Boolean = false): Unit = {
    // scalastyle:off println
    println(toSQLFlowString(contracted))
    // scalastyle:on println
  }
}
