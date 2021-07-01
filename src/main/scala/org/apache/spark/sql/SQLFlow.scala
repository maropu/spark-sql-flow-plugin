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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, LeftExistence}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.LogicalRelation

abstract class BaseSQLFlow extends PredicateHelper with Logging {

  private val nextNodeId = new AtomicInteger(0)

  def collectEdges(
    tempView: String,
    plan: LogicalPlan,
    nodeMap: mutable.Map[String, String]): Seq[String]

  def catalogToSQLFlow(session: SparkSession): String = {
    val catalog = session.sessionState.catalog
    val tempViewMap = catalog.getTempViewNames().map { tempView =>
      tempView -> catalog.getTempView(tempView).get
    }.toMap

    val nodeMap = mutable.Map[String, String]()
    val edges = catalog.getTempViewNames.map { tempView =>
      val analyzed = session.sessionState.analyzer.execute(tempViewMap(tempView))
      val optimized = session.sessionState.optimizer.execute(analyzed.transformDown {
        case s @ SubqueryAlias(AliasIdentifier(name, Nil), _) if tempViewMap.contains(name) =>
          TempView(name, s.output)
      })

      // Generate a node label for a temporary view if necessary
      nodeMap.getOrElseUpdate(tempView, generateNodeString(optimized, tempView, "lightyellow"))

      if (!optimized.isInstanceOf[TempView]) {
        collectEdges(tempView, optimized, nodeMap)
      } else {
        // If a given plan is `TempView t1, [a#102, b#103]`, `nodeName` should be equal to
        // `tempView` and we don't need a new node and edges for `TempView`.
        Nil
      }
    }

    generateGraphString(nodeMap.values.toSeq, edges.flatten)
  }

  private def isCached(name: String): Boolean = {
    SparkSession.getActiveSession.exists { session =>
      session.sessionState.catalog.getTempView(name).exists { p =>
        val analyzed = session.sessionState.analyzer.execute(p)
        session.sharedState.cacheManager.lookupCachedData(analyzed).isDefined
      }
    }
  }

  protected def getNodeName(p: LogicalPlan) = p match {
    case TempView(name, _) => name
    case LogicalRelation(_, _, Some(table), false) => table.qualifiedName
    case HiveTableRelation(table, _, _, _, _) => table.qualifiedName
    case j: Join => s"${p.nodeName}_${j.joinType}_${nextNodeId.getAndIncrement()}"
    case _ => s"${p.nodeName}_${nextNodeId.getAndIncrement()}"
  }

  private def getNodeColor(plan: LogicalPlan): String = plan match {
    case TempView(name, _) if isCached(name) => "lightblue"
    case _: TempView => "lightyellow"
    case _: LeafNode => "lightpink"
    case _ => "lightgray"
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
         |  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
         |  node [shape=plain]
         |  rankdir=LR;
         |
         |  ${nodes.sorted.mkString("\n")}
         |  ${edges.sorted.mkString("\n")}
         |}
       """.stripMargin
    } else {
      ""
    }
  }

  protected def generateNodeString(p: LogicalPlan, nodeName: String, nodeColor: String = "") = {
    val outputAttrs = p.output.zipWithIndex.map { case (attr, i) =>
      s"""<tr><td port="$i">${normalizeForHtml(attr.name)}</td></tr>"""
    }
    // scalastyle:off line.size.limit
    s"""
       |"$nodeName" [label=<
       |<table border="1" cellborder="0" cellspacing="0">
       |  <tr><td bgcolor="${if (nodeColor.isEmpty) getNodeColor(p) else nodeColor}" port="nodeName"><i>$nodeName</i></td></tr>
       |  ${outputAttrs.mkString("\n")}
       |</table>>];
     """.stripMargin
    // scalastyle:on line.size.limit
  }
}

case class SQLFlow() extends BaseSQLFlow {

  override def collectEdges(
      tempView: String,
      plan: LogicalPlan,
      nodeMap: mutable.Map[String, String]): Seq[String] = {
    val (_, outputAttrMap, edgeEntries) = traversePlanRecursively(plan, nodeMap)
    val tempViewEdges = outputAttrMap.zipWithIndex.map {
      case ((_, input), i) => s"""$input -> "$tempView":$i;"""
    }
    edgeEntries ++ tempViewEdges
  }

  def planToSQLFlow(plan: LogicalPlan): String = {
    val nodeMap = mutable.Map[String, String]()
    val (_, _, edges) = traversePlanRecursively(plan, nodeMap)
    generateGraphString(nodeMap.values.toSeq, edges)
  }

  private def collectEdges(
      nodeName: String,
      plan: LogicalPlan,
      inputAttrSeq: Seq[Seq[(Attribute, String)]],
      outputAttrs: Seq[(Attribute, Int)]): Seq[String] = {
    lazy val inputAttrMap = AttributeMap(inputAttrSeq.flatten)
    plan match {
      case Aggregate(_, aggExprs, _) =>
        aggExprs.zip(outputAttrs).flatMap { case (ne, (_, i)) =>
          ne.references.filter(inputAttrMap.contains).map { attr =>
            s"""${inputAttrMap(attr)} -> "$nodeName":$i;"""
          }
        }

      case Project(projList, _) =>
        projList.zip(outputAttrs).flatMap { case (ne, (_, i)) =>
          ne.references.filter(inputAttrMap.contains).map { attr =>
            s"""${inputAttrMap(attr)} -> "$nodeName":$i;"""
          }
        }

      case g @ Generate(generator, _, _, _, generatorOutput, _) =>
        val edgesForChildren = g.requiredChildOutput.zipWithIndex.flatMap { case (attr, i) =>
          inputAttrMap.get(attr).map { input => s"""$input -> "$nodeName":$i;"""}
        }
        val edgeForGenerator = generator.references.flatMap(inputAttrMap.get).headOption
          .map { genInput =>
            generatorOutput.zipWithIndex.map { case (attr, i) =>
              s"""$genInput -> "$nodeName":${g.requiredChildOutput.size + i}"""
            }
          }
        edgesForChildren ++ edgeForGenerator.seq.flatten

      case Expand(projections, _, _) =>
        projections.transpose.zipWithIndex.flatMap { case (projs, i) =>
          projs.flatMap(e => e.references.flatMap(inputAttrMap.get))
            .map { input => s"""$input -> "$nodeName":$i;"""}
            .distinct
        }

      case _: Union =>
        inputAttrSeq.transpose.zipWithIndex.flatMap { case (attrs, i) =>
          attrs.map { case (_, input) => s"""$input -> "$nodeName":$i"""}
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
                  s"""$input -> "$nodeName":${leftAttrIndexMap(attr)};"""
                }
              }
            }
            val joinOutputEdges = left.map(_._2).zipWithIndex.map { case (input, i) =>
              s"""$input -> "$nodeName":$i;"""
            }
            joinOutputEdges ++ predicateEdges.getOrElse(Nil)
          case _ =>
            (left ++ right).map(_._2).zipWithIndex.map { case (input, i) =>
              s"""$input -> "$nodeName":$i;"""
            }
        }

      case _ =>
        outputAttrs.flatMap { case (attr, i) =>
          inputAttrMap.get(attr).map { input => s"""$input -> "$nodeName":$i;"""}
        }
    }
  }

  private def traversePlanRecursively(plan: LogicalPlan, nodeMap: mutable.Map[String, String])
    : (String, Seq[(Attribute, String)], Seq[String]) = plan match {
    case _: LeafNode =>
      val nodeName = getNodeName(plan)
      nodeMap.getOrElseUpdate(nodeName, generateNodeString(plan, nodeName))
      val outputAttrMap = plan.output.zipWithIndex.map { case (attr, i) =>
        attr -> s""""$nodeName":$i"""
      }
      (nodeName, outputAttrMap, Nil)

    case _ =>
      val inputInfos = plan.children.map(traversePlanRecursively(_, nodeMap))
      val nodeName = getNodeName(plan)

      // Handles subqueries if necessary
      val subqueries = plan.expressions.flatMap(_.collect { case ss: ScalarSubquery => ss })
      val edgesInSubqueries = if (subqueries.nonEmpty) {
        subqueries.flatMap { ss =>
          val (_, outputAttrMap, e) = traversePlanRecursively(ss.plan, nodeMap)
          val edges = e ++ outputAttrMap.map { case (_, src) =>
            s"""$src -> "$nodeName":nodeName"""
          }
          edges
        }
      } else {
        Nil
      }

      if (plan.output.nonEmpty) {
        val outputAttrsWithIndex = plan.output.zipWithIndex
        val outputAttrMap = outputAttrsWithIndex.map { case (attr, i) =>
          attr -> s""""$nodeName":$i"""
        }
        val edgeInfo = collectEdges(nodeName, plan, inputInfos.map(_._2), outputAttrsWithIndex)
        nodeMap.getOrElseUpdate(nodeName, generateNodeString(plan, nodeName))
        (nodeName, outputAttrMap, edgeInfo ++ inputInfos.flatMap(_._3) ++ edgesInSubqueries)
      } else {
        (nodeName, Nil, inputInfos.flatMap(_._3) ++ edgesInSubqueries)
      }
  }
}

case class SQLContractedFlow() extends BaseSQLFlow {

  override def collectEdges(
      tempView: String,
      plan: LogicalPlan,
      nodeMap: mutable.Map[String, String]): Seq[String] = {
    val outputAttrMap = plan.output.map(_.exprId).zipWithIndex.toMap
    val (edges, candidateEdges) = traversePlanRecursively(tempView, plan, nodeMap)
    edges ++ candidateEdges.flatMap { case (src, exprId) =>
      if (outputAttrMap.contains(exprId)) {
        Some(s"$src -> $tempView:${outputAttrMap(exprId)}")
      } else {
        None
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
          val warningMsg =
            s"""refMap does not have enough entries for ${plan.nodeName}:
               |missingRefs: ${missingRefs.mkString(", ")}
               |${plan.treeString}
             """.stripMargin
          if (!SQLFlow.isTesting) {
            logWarning(warningMsg)
          } else {
            assert(false, warningMsg)
          }
        }
    }
  }

  private def traversePlanRecursively(
      tempView: String,
      plan: LogicalPlan,
      nodeMap: mutable.Map[String, String]): (Seq[String], Seq[(String, ExprId)]) = {
    // Collect input nodes
    val inputNodes = plan.collectLeaves().map { p =>
      val nodeName = getNodeName(p)
      nodeMap.getOrElseUpdate(nodeName, generateNodeString(p, nodeName))
      (nodeName, p.output)
    }.groupBy(_._1).map { case (_, v) =>
      v.head
    }

    // Handles subqueries if necessary
    val subqueries = plan.collect { case p =>
      p.expressions.flatMap(_.collect { case ss: ScalarSubquery => ss })
    }.flatten
    val edgesInSubqueries = if (subqueries.nonEmpty) {
      subqueries.flatMap { ss =>
        val outputAttrSet = ss.plan.output.map(_.exprId).toSet
        val (edges, candidateEdges) = traversePlanRecursively(tempView, ss.plan, nodeMap)
        edges ++ candidateEdges.flatMap { case (src, exprId) =>
          if (outputAttrSet.contains(exprId)) {
            Some(s"$src -> $tempView:nodeName")
          } else {
            None
          }
        }
      }
    } else {
      Nil
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

    val candidateEdges = inputNodes.toSeq.flatMap { case (inputNodeId, input) =>
      input.zipWithIndex.flatMap { case (a, i) =>
        traverseInRefMap(a.exprId).map { exprId =>
          (s""""$inputNodeId":$i""", exprId)
        }
      }
    }
    (edgesInSubqueries, candidateEdges)
  }
}

case class TempView(name: String, output: Seq[Attribute]) extends LeafNode {
  override lazy val resolved: Boolean = true
}

case class SQLFlowHolder[T] private[sql](private val ds: Dataset[T]) {
  import SQLFlow._

  def debugPrintAsSQLFlow(): Unit = {
    // scalastyle:off println
    println(SQLFlow().planToSQLFlow(ds.queryExecution.optimizedPlan))
    // scalastyle:on println
  }

  def saveAsSQLFlow(path: String, format: String = "svg"): Unit = {
    val flowString = SQLFlow().planToSQLFlow(ds.queryExecution.optimizedPlan)
    writeSQLFlow(path, format, flowString)
  }
}

object SQLFlow extends PredicateHelper with Logging {

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

  def writeSQLFlow(path: String, format: String, flowString: String): Unit = {
    if (!SQLFlow.validImageFormatSet.contains(format.toLowerCase(Locale.ROOT))) {
      throw new AnalysisException(s"Invalid image format: $format")
    }
    val outputDir = new File(path)
    if (!outputDir.mkdir()) {
      throw new AnalysisException(s"path file:$path already exists")
    }
    val filePrefix = "sqlflow"
    val dotFile = stringToFile(new File(outputDir, s"$filePrefix.dot"), flowString)
    val srcFile = dotFile.getAbsolutePath
    val dstFile = new File(outputDir, s"$filePrefix.$format").getAbsolutePath
    tryGenerateImageFile(format, srcFile, dstFile)
  }

  def saveAsSQLFlow(path: String, format: String = "svg", contracted: Boolean = false): Unit = {
    SparkSession.getActiveSession.map { session =>
      val flowString = if (contracted) {
        SQLContractedFlow().catalogToSQLFlow(session)
      } else {
        SQLFlow().catalogToSQLFlow(session)
      }
      SQLFlow.writeSQLFlow(path, format, flowString)
    }.getOrElse {
      logWarning(s"Active SparkSession not found")
    }
  }

  // Indicates whether Spark is currently running unit tests
  private[sql] lazy val isTesting: Boolean = {
    System.getenv("SPARK_TESTING") != null || System.getProperty("spark.testing") != null
  }

  def debugPrintAsSQLFlow(contracted: Boolean = false): Unit = {
    SparkSession.getActiveSession.map { session =>
      val flowString = if (contracted) {
        SQLContractedFlow().catalogToSQLFlow(session)
      } else {
        SQLFlow().catalogToSQLFlow(session)
      }
      // scalastyle:off println
      println(flowString)
      // scalastyle:on println
    }.getOrElse {
      logWarning(s"Active SparkSession not found")
    }
  }
}
