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

package org.apache.spark.sql.flow

import java.security.MessageDigest
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.util.TimeZone

import scala.collection.mutable

import org.apache.commons.lang3.RandomStringUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, JoinType, LeftExistence}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.flow.sink.{BaseGraphFormat, GraphVizSink}
import org.apache.spark.sql.internal.SQLConf

abstract class BaseSQLFlow extends PredicateHelper with Logging {

  def collectEdges(
    tempView: String,
    plan: LogicalPlan,
    nodeMap: mutable.Map[String, SQLFlowGraphNode]): Seq[SQLFlowGraphEdge]

  def planToSQLFlow(plan: LogicalPlan, flowName: Option[String] = None)
    : (Seq[SQLFlowGraphNode], Seq[SQLFlowGraphEdge]) = {
    val nodeMap = mutable.Map[String, SQLFlowGraphNode]()
    val dstUniqId = s"query_${SQLFlow.nodeUniqueId()}"
    val dstNodeName = flowName.getOrElse(s"query_${Math.abs(plan.semanticHash())}")
    val outputAttrNames = plan.output.map(_.name)
    val schema = plan.schema.toDDL
    val dstNode = generateQueryNode(outputAttrNames, dstUniqId, dstNodeName, schema)
    val edges = collectEdges(dstUniqId, plan, nodeMap)
    (dstNode +: nodeMap.values.toSeq, edges)
  }

  def catalogToSQLFlow(session: SparkSession): (Seq[SQLFlowGraphNode], Seq[SQLFlowGraphEdge]) = {
    val nodeMap = mutable.Map[String, SQLFlowGraphNode]()

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
      val outputAttrNames = analyzed.output.map(_.name)
      val schema = analyzed.schema.toDDL
      val node = generateViewNode(outputAttrNames, viewName, viewName, schema, isCached(analyzed))
      nodeMap(viewName) = node
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
          case sq @ ScalarSubquery(sp, _, _, _) =>
            sq.copy(plan = sp.transformDown {
              case p => replaceWithTempViewNode(p)
            })

          case sq @ Exists(sp, _, _, _) =>
            sq.copy(plan = sp.transformDown {
              case p => replaceWithTempViewNode(p)
            })

          case sq @ InSubquery(_, q @ ListQuery(sp, _, _, _, _)) =>
            sq.copy(query = q.copy(plan = sp.transformDown {
              case p => replaceWithTempViewNode(p)
            }))

          case sq @ LateralSubquery(sp, _, _, _) =>
            sq.copy(plan = sp.transformDown {
              case p => replaceWithTempViewNode(p)
            })
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

    (nodeMap.values.toSeq, edges.flatten)
  }

  protected def isCached(plan: LogicalPlan): Boolean = {
    val session = SparkSession.getActiveSession.getOrElse {
      throw new IllegalStateException("Active SparkSession not found")
    }
    session.sharedState.cacheManager.lookupCachedData(plan).isDefined
  }

  protected def getNodeNameWithId(name: String): String = {
    s"${name}_${SQLFlow.nodeUniqueId()}"
  }

  private def joinTypeName(jt: JoinType): String = jt match {
    case ExistenceJoin(_) => "ExistenceJoin"
    case _ => jt.toString
  }

  protected def getNodeName(p: LogicalPlan) = p match {
    case ViewNode(name, _) =>
      (name, name)
    case TempViewNode(name, _) =>
      (name, name)
    case LogicalRelation(_, _, Some(table), false) =>
      (table.qualifiedName, table.qualifiedName)
    case HiveTableRelation(table, _, _, _, _) =>
      (table.qualifiedName, table.qualifiedName)
    case j: Join =>
      val nodeName = s"${p.nodeName}_${joinTypeName(j.joinType)}"
      (nodeName, getNodeNameWithId(nodeName))
    case _ =>
      (p.nodeName, getNodeNameWithId(p.nodeName))
  }

  protected def generateTableNode(
      outputAttrNames: Seq[String],
      uniqId: String,
      nodeName: String,
      schemaDDL: String,
      isCached: Boolean = false): SQLFlowGraphNode = {
    SQLFlowGraphNode(
      uniqId,
      nodeName,
      outputAttrNames,
      schemaDDL,
      GraphNodeType.TableNode,
      isCached)
  }

  protected def generateViewNode(
      outputAttrNames: Seq[String],
      uniqId: String,
      nodeName: String,
      schemaDDL: String,
      isCached: Boolean = false): SQLFlowGraphNode = {
    SQLFlowGraphNode(
      uniqId,
      nodeName,
      outputAttrNames,
      schemaDDL,
      GraphNodeType.ViewNode,
      isCached)
  }

  protected def generatePlanNode(
      outputAttrNames: Seq[String],
      uniqId: String,
      nodeName: String,
      schemaDDL: String,
      isLeaf: Boolean = false,
      isCached: Boolean = false): SQLFlowGraphNode = {
    SQLFlowGraphNode(
      uniqId,
      nodeName,
      outputAttrNames,
      schemaDDL,
      if (isLeaf) GraphNodeType.LeafPlanNode else GraphNodeType.PlanNode,
      isCached)
  }

  protected def generateQueryNode(
      outputAttrNames: Seq[String],
      uniqId: String,
      nodeName: String,
      schemaDDL: String): SQLFlowGraphNode = {
    SQLFlowGraphNode(
      uniqId,
      nodeName,
      outputAttrNames,
      schemaDDL,
      GraphNodeType.QueryNode,
      false)
  }

  private def toUTC(t: Long): String = {
    val zoneId = TimeZone.getTimeZone( "UTC" ).toZoneId
    new Timestamp(t).toLocalDateTime.atZone(zoneId).format(DateTimeFormatter.ISO_INSTANT)
  }

  private def getCreateTime(p: LogicalPlan): Option[String] = p match {
    case r: LogicalRelation if r.catalogTable.isDefined =>
      Some(toUTC(r.catalogTable.get.createTime))
    case r: HiveTableRelation =>
      Some(toUTC(r.tableMeta.createTime))
    case _ =>
      None
  }

  private def getStatsFromLeafPlan(p: LogicalPlan): Seq[(String, String)] = p match {
    case _: LogicalRelation | _: HiveTableRelation =>
      val stats = mutable.ArrayBuffer[(String, String)]()
      val planStats = p.asInstanceOf[LeafNode].computeStats()
      stats += "sizeInBytes" -> s"${planStats.sizeInBytes}"
      planStats.rowCount.foreach { cnt =>
        stats += "rowCount" -> s"$cnt"
      }
      stats.toSeq
    case _ =>
      Nil
  }

  private def setPlanPropsIn(node: SQLFlowGraphNode, p: LogicalPlan): Unit = {
    getCreateTime(p).foreach { t => node.props += "createTime" -> t }
    getStatsFromLeafPlan(p).foreach { kv => node.props += kv }
    node.props ++= Map("semanticHash" -> SQLFlow.semanticHash(p))
  }

  protected def generateGraphNode(
      p: LogicalPlan,
      uniqId: String,
      nodeName: String,
      isCached: Boolean): SQLFlowGraphNode = {
    val outputAttrNames = p.output.map(_.name)
    val schemaDDL = p.schema.toDDL
    val graphNode = p match {
      case _: LocalRelation | _: LogicalRelation | _: InMemoryRelation | _: HiveTableRelation =>
        generateTableNode(outputAttrNames, uniqId, nodeName, schemaDDL, isCached)
      case _: View | _: ViewNode | _: TempViewNode =>
        generateViewNode(outputAttrNames, uniqId, nodeName, schemaDDL, isCached)
      case _ =>
        val isLeaf = p match {
          case _: Range => true
          case _ => false
        }
        generatePlanNode(outputAttrNames, uniqId, nodeName, schemaDDL, isLeaf, isCached)
    }
    setPlanPropsIn(graphNode, p)
    graphNode
  }
}

case class SQLFlow() extends BaseSQLFlow {

  override def collectEdges(
      tempView: String,
      plan: LogicalPlan,
      nodeMap: mutable.Map[String, SQLFlowGraphNode]): Seq[SQLFlowGraphEdge] = {
    val (inputNodeId, edges) = traversePlanRecursively(plan, nodeMap, isRoot = true)
    val edgesToTempView = if (inputNodeId != tempView) {
      plan.output.indices.map { i =>
        SQLFlowGraphEdge(inputNodeId, Some(i), tempView, Some(i))
      }
    } else {
      Nil
    }
    edges ++ edgesToTempView
  }

  private def collectEdgesInPlan(
      plan: LogicalPlan,
      curNodeName: String,
      inputNodeIds: Seq[String]): Seq[SQLFlowGraphEdge] = {
    val inputNodeIdsWithOutput = inputNodeIds.zip(plan.children.map(_.output))
    val inputAttrSeq = inputNodeIdsWithOutput.map { case (nodeId, output) =>
      output.zipWithIndex.map { case (a, i) =>
        a -> (nodeId, i)
      }
    }
    val inputAttrMap = AttributeMap(inputAttrSeq.flatten)
    val outputAttrWithIndex = plan.output.zipWithIndex
    val edges = plan match {
      case Aggregate(_, aggExprs, _) =>
        aggExprs.zip(outputAttrWithIndex).flatMap { case (ne, (_, i)) =>
          ne.references.filter(inputAttrMap.contains).map { attr =>
            val (inputNodeId, fromIdx) = inputAttrMap(attr)
            SQLFlowGraphEdge(inputNodeId, Some(fromIdx), curNodeName, Some(i))
          }
        }

      case Project(projList, _) =>
        projList.zip(outputAttrWithIndex).flatMap { case (ne, (_, i)) =>
          ne.references.filter(inputAttrMap.contains).map { attr =>
            val (inputNodeId, fromIdx) = inputAttrMap(attr)
            SQLFlowGraphEdge(inputNodeId, Some(fromIdx), curNodeName, Some(i))
          }
        }

      case g @ Generate(generator, _, _, _, generatorOutput, _) =>
        val edgesForChildren = g.requiredChildOutput.zipWithIndex.flatMap { case (attr, i) =>
          inputAttrMap.get(attr).map { case (inputNodeId, fromIdx) =>
            SQLFlowGraphEdge(inputNodeId, Some(fromIdx), curNodeName, Some(i))
          }
        }
        val edgeForGenerator = generator.references.flatMap(inputAttrMap.get).headOption
          .map { case (inputNodeId, fromIdx) =>
            generatorOutput.zipWithIndex.map { case (attr, i) =>
              val toIdx = g.requiredChildOutput.size + i
              SQLFlowGraphEdge(inputNodeId, Some(fromIdx), curNodeName, Some(toIdx))
            }
          }
        edgesForChildren ++ edgeForGenerator.seq.flatten

      case Expand(projections, _, _) =>
        projections.transpose.zipWithIndex.flatMap { case (projs, i) =>
          projs.flatMap(e => e.references.flatMap(inputAttrMap.get))
            .map { case (inputNodeId, fromIdx) =>
              SQLFlowGraphEdge(inputNodeId, Some(fromIdx), curNodeName, Some(i))
            }.distinct
        }

      case _: Union =>
        inputAttrSeq.transpose.zipWithIndex.flatMap { case (attrs, i) =>
          attrs.map { case (_, (inputNodeId, fromIdx)) =>
            SQLFlowGraphEdge(inputNodeId, Some(fromIdx), curNodeName, Some(i))
          }
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
              right.flatMap { case (attr, (inputNodeId, fromIdx)) =>
                val leftAttrs = referenceSeq.flatMap { refs =>
                  if (refs.contains(attr)) {
                    refs.intersect(leftAttrSet).toSeq
                  } else {
                    Nil
                  }
                }
                leftAttrs.map { attr =>
                  val toIdx = leftAttrIndexMap(attr)
                  SQLFlowGraphEdge(inputNodeId, Some(fromIdx), curNodeName, Some(toIdx))
                }
              }
            }
            val joinOutputEdges = left.map(_._2).zipWithIndex.map {
              case ((inputNodeId, fromIdx), i) =>
                SQLFlowGraphEdge(inputNodeId, Some(fromIdx), curNodeName, Some(i))
            }
            joinOutputEdges ++ predicateEdges.getOrElse(Nil)
          case _ =>
            (left ++ right).map(_._2).zipWithIndex.map {
              case ((inputNodeId, fromIdx), i) =>
                SQLFlowGraphEdge(inputNodeId, Some(fromIdx), curNodeName, Some(i))
            }
        }

      // TODO: Needs to check if the other Python-related plan nodes are handled correctly
      case _: FlatMapGroupsInPandas =>
        inputAttrSeq.head.zip(outputAttrWithIndex).map {
          case ((_, (inputNodeId, fromIdx)), (_, i)) =>
            SQLFlowGraphEdge(inputNodeId, Some(fromIdx), curNodeName, Some(i))
        }

      case _ =>
        outputAttrWithIndex.flatMap { case (attr, i) =>
          inputAttrMap.get(attr).map { case (inputNodeId, fromIdx) =>
            SQLFlowGraphEdge(inputNodeId, Some(fromIdx), curNodeName, Some(i))
          }
        }
    }

    if (edges.isEmpty) {
      inputNodeIdsWithOutput.flatMap { case (inputNodeId, output) =>
        if (output.isEmpty) {
          SQLFlowGraphEdge(inputNodeId, None, curNodeName, None) :: Nil
        } else {
          output.zipWithIndex.map { case (_, i) =>
            SQLFlowGraphEdge(inputNodeId, Some(i), curNodeName, None)
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
      nodeMap: mutable.Map[String, SQLFlowGraphNode]): Seq[SQLFlowGraphEdge] = {
    val hasSbuqueres = plan.expressions.exists(SubqueryExpression.hasSubquery)
    if (hasSbuqueres) {
      val planOutputMap = AttributeMap(plan.output.zipWithIndex)

      def collectEdgesInExprs(ne: NamedExpression): Seq[SQLFlowGraphEdge] = {
        val attr = ne.toAttribute
        val subquries = ne.collect { case ss: ScalarSubquery => ss }
        subquries.flatMap { ss =>
          val (inputNodeId, edges) = traversePlanRecursively(ss.plan, nodeMap)
          edges ++ ss.plan.output.indices.map { i =>
            if (planOutputMap.contains(attr)) {
              SQLFlowGraphEdge(inputNodeId, Some(i), nodeName, Some(planOutputMap(attr)))
            } else {
              SQLFlowGraphEdge(inputNodeId, Some(i), nodeName, None)
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
                  Some(SQLFlowGraphEdge(inputNodeId, Some(i), nodeName, Some(planOutputMap(attr))))
                } else {
                  None
                }
              }

              if (edgesInSubqueries.nonEmpty) {
                edgesInSubqueries
              } else {
                SQLFlowGraphEdge(inputNodeId, Some(i), nodeName, None) :: Nil
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
              SQLFlowGraphEdge(inputNodeId, Some(i), nodeName, None)
            }
          }
      }
    } else {
      Nil
    }
  }

  private def getOrCreateNode(
      plan: LogicalPlan,
      nodeMap: mutable.Map[String, SQLFlowGraphNode],
      cached: Boolean = false): String = {
    val (nodeName, uniqId) = getNodeName(plan)
    // Generate a node label for a plan if necessary
    nodeMap.getOrElseUpdate(uniqId, generateGraphNode(plan, uniqId, nodeName, cached))
    uniqId
  }

  private def traversePlanRecursively(
    plan: LogicalPlan,
    nodeMap: mutable.Map[String, SQLFlowGraphNode],
    cached: Boolean = false,
    isRoot: Boolean = false): (String, Seq[SQLFlowGraphEdge]) = plan match {
    case _: LeafNode =>
      val nodeName = getOrCreateNode(plan, nodeMap)
      (nodeName, Nil)

    case CachedNode(cachedPlan) =>
      traversePlanRecursively(cachedPlan, nodeMap, cached = !isRoot)

    case _ =>
      val edgesInChildren = plan.children.map(traversePlanRecursively(_, nodeMap))
      if (plan.output.nonEmpty) {
        val nodeName = getOrCreateNode(plan, nodeMap, cached)
        val edges = collectEdgesInPlan(plan, nodeName, edgesInChildren.map(_._1))
        val edgesInSubqueries = collectEdgesInSubqueries(nodeName, plan, nodeMap)
        (nodeName, edges ++ edgesInChildren.flatMap(_._2) ++ edgesInSubqueries)
      } else {
        val nodeName = getOrCreateNode(plan, nodeMap)
        val edges = edgesInChildren.map(_._1).zip(plan.children.map(_.output))
          .flatMap { case (inputNodeId, output) =>
            output.zipWithIndex.map { case (_, i) =>
              SQLFlowGraphEdge(inputNodeId, Some(i), nodeName, None)
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
      nodeMap: mutable.Map[String, SQLFlowGraphNode]): Seq[SQLFlowGraphEdge] = {
    val outputAttrMap = plan.output.map(_.exprId).zipWithIndex.toMap
    val (edges, candidateEdges, refMap) = traversePlanRecursively(tempView, plan, nodeMap)
    edges ++ candidateEdges.flatMap { case ((inputNodeId, input), candidates) =>
      val edges = candidates.flatMap { case ((inputNodeId, fromIdx), exprId) =>
        if (inputNodeId != tempView && outputAttrMap.contains(exprId)) {
          val toIdx = outputAttrMap(exprId)
          Some(SQLFlowGraphEdge(inputNodeId, Some(fromIdx), tempView, Some(toIdx)))
        } else {
          None
        }
      }
      if (edges.isEmpty) {
        // TODO: Makes it more precise
        input.zipWithIndex.filter { i => refMap.contains(i._1.exprId) }.map { case (_, i) =>
          SQLFlowGraphEdge(inputNodeId, Some(i), tempView, None)
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
      nodeMap: mutable.Map[String, SQLFlowGraphNode]): Seq[SQLFlowGraphEdge] = {
    val subqueries = plan.collect { case p =>
      p.expressions.flatMap(_.collect { case ss: ScalarSubquery => ss })
    }.flatten
    if (subqueries.nonEmpty) {
      // TODO: Needs to handle Project/Aggregate/Filter nodes
      subqueries.flatMap { ss =>
        val outputAttrSet = ss.plan.output.map(_.exprId).toSet
        val (edges, candidateEdges, refMap) = traversePlanRecursively(tempView, ss.plan, nodeMap)
        edges ++ candidateEdges.flatMap { case ((inputNodeId, input), candidates) =>
          val edges = candidates.flatMap { case ((inputNodeId, i), exprId) =>
            if (outputAttrSet.contains(exprId)) {
              Some(SQLFlowGraphEdge(inputNodeId, Some(i), tempView, None))
            } else {
              None
            }
          }
          if (edges.isEmpty) {
            // TODO: Makes it more precise
            input.zipWithIndex.filter { i => refMap.contains(i._1.exprId) }.map { case (_, i) =>
              SQLFlowGraphEdge(inputNodeId, Some(i), tempView, None)
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
      nodeMap: mutable.Map[String, SQLFlowGraphNode])
    : (Seq[SQLFlowGraphEdge], Seq[((String, Seq[Attribute]), Seq[((String, Int), ExprId)])],
      Map[ExprId, Set[ExprId]]) = {
    // Collect input nodes
    val inputNodes = plan.collectLeaves().map { p =>
      val (nodeName, uniqId) = getNodeName(p)
      nodeMap.getOrElseUpdate(uniqId, generateGraphNode(p, uniqId, nodeName, isCached(p)))
      (uniqId, p.output)
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
          ((inputNodeId, i), exprId)
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
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(cachedPlan = newChild)
  }
}

case class ViewNode(name: String, output: Seq[Attribute]) extends LeafNode {
  override lazy val resolved: Boolean = true
}

case class TempViewNode(name: String, output: Seq[Attribute]) extends LeafNode {
  override lazy val resolved: Boolean = true
}

case class SQLFlowHolder[T] private[sql](private val ds: Dataset[T]) {

  def printAsSQLFlow(
      contracted: Boolean = false,
      graphFormat: BaseGraphFormat = GraphVizSink()): Unit = {
    // scalastyle:off println
    val sqlFlow = if (contracted) SQLContractedFlow() else SQLFlow()
    val (nodes, edges) = sqlFlow.planToSQLFlow(ds.queryExecution.optimizedPlan)
    println(graphFormat.toGraphString(nodes, edges))
    // scalastyle:on println
  }

  def saveAsSQLFlow(
      options: Map[String, String] = Map.empty[String, String],
      contracted: Boolean = false,
      graphSink: BaseGraphBatchSink = GraphVizSink()): Unit = {
    val sqlFlow = if (contracted) SQLContractedFlow() else SQLFlow()
    val (nodes, edges) = sqlFlow.planToSQLFlow(ds.queryExecution.optimizedPlan)
    graphSink.write(nodes, edges, options)
  }
}

object SQLFlow extends Logging {

  implicit def DatasetToSQLFlowHolder[T](ds: Dataset[T]): SQLFlowHolder[T] = {
    new SQLFlowHolder[T](ds)
  }

  private[spark] def toSQLFlow(contracted: Boolean = false)
    : (Seq[SQLFlowGraphNode], Seq[SQLFlowGraphEdge]) = {
    SparkSession.getActiveSession.map { session =>
      if (contracted) {
        SQLContractedFlow().catalogToSQLFlow(session)
      } else {
        SQLFlow().catalogToSQLFlow(session)
      }
    }.getOrElse {
      logWarning("Active SparkSession not found")
      (Nil, Nil)
    }
  }

  private def computeDigest(seed: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(seed.getBytes)
    val bytes = md.digest()
    val sb = new mutable.StringBuilder()
    bytes.foreach { b =>
      sb.append("%02x".format(b & 0xff))
    }
    sb.toString
  }

  private[flow] def nodeUniqueId(): String = {
    val seed = RandomStringUtils.random(128, true, true)
    computeDigest(seed).substring(0, 7)
  }

  private def withSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      if (SQLConf.isStaticConfigKey(k)) {
        throw new AnalysisException(s"Cannot modify the value of a static config: $k")
      }
      conf.setConfString(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  // TODO: Revisit this; we need to return same hash values
  // between different JVM instances.
  private [flow] def semanticHash(p: LogicalPlan): String = {
    withSQLConf((SQLConf.MAX_TO_STRING_FIELDS.key, "1024")) {
      computeDigest(p.canonicalized.toString).substring(0, 7)
    }
  }

  def saveAsSQLFlow(
      options: Map[String, String] = Map.empty[String, String],
      contracted: Boolean = false,
      graphSink: BaseGraphBatchSink = GraphVizSink()): Unit = {
    val (nodes, edges) = toSQLFlow(contracted)
    graphSink.write(nodes, edges, options)
  }

  // Indicates whether Spark is currently running unit tests
  private[flow] lazy val isTesting: Boolean = {
    System.getenv("SPARK_TESTING") != null || System.getProperty("spark.testing") != null
  }

  def printAsSQLFlow(
      contracted: Boolean = false,
      graphFormat: BaseGraphFormat = GraphVizSink()): Unit = {
    // scalastyle:off println
    val (nodes, edges) = toSQLFlow(contracted)
    println(graphFormat.toGraphString(nodes, edges))
    // scalastyle:on println
  }
}
