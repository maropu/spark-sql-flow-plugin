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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeSet, BinaryComparison, ExprId, PredicateHelper, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.LeftExistence
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class SQLFlowHolder[T] private[sql](private val ds: Dataset[T]) {
  import SQLFlow._

  def debugPrintAsSQLFlow(): Unit = {
    // scalastyle:off println
    println(planToSQLFlow(ds.queryExecution.optimizedPlan))
    // scalastyle:on println
  }

  def saveAsSQLFlow(path: String, format: String = "svg"): Unit = {
    val flowString = planToSQLFlow(ds.queryExecution.optimizedPlan)
    writeSQLFlow(path, format, flowString)
  }
}

case class TempView(name: String, output: Seq[Attribute]) extends LeafNode {
  override lazy val resolved: Boolean = true
}

object SQLFlow extends PredicateHelper with Logging {

  private val nextNodeId = new AtomicInteger(0)

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

  private[sql] val validImageFormatSet = Set("png", "svg")

  private[sql] def writeSQLFlow(path: String, format: String, flowString: String): Unit = {
    if (!validImageFormatSet.contains(format.toLowerCase(Locale.ROOT))) {
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

  def debugPrintAsSQLFlow(contracted: Boolean = false): Unit = {
    SparkSession.getActiveSession.map { session =>
      val flowString = if (contracted) {
        catalogToContractedSQLFlow(session)
      } else {
        catalogToSQLFlow(session)
      }
      // scalastyle:off println
      println(flowString)
      // scalastyle:on println
    }.getOrElse {
      logWarning(s"Active SparkSession not found")
    }
  }

  def saveAsSQLFlow(path: String, format: String = "svg", contracted: Boolean = false): Unit = {
    SparkSession.getActiveSession.map { session =>
      val flowString = if (contracted) {
        catalogToContractedSQLFlow(session)
      } else {
        catalogToSQLFlow(session)
      }
      writeSQLFlow(path, format, flowString)
    }.getOrElse {
      logWarning(s"Active SparkSession not found")
    }
  }

  private[sql] def catalogToContractedSQLFlow(session: SparkSession): String = {
    val catalog = session.sessionState.catalog
    val tempViewMap = catalog.getTempViewNames().map { tempView =>
      tempView -> catalog.getTempView(tempView).get
    }.toMap

    val (nodes, edges) = catalog.getTempViewNames.map { tempView =>
      val analyzed = session.sessionState.analyzer.execute(tempViewMap(tempView))
      val normalized = analyzed.transformDown {
        case s @ SubqueryAlias(AliasIdentifier(name, Nil), _) if tempViewMap.contains(name) =>
          TempView(name, s.output)
      }

      val optimized = session.sessionState.optimizer.execute(normalized)

      // Collect input nodes
      val inputNodes = optimized.collectLeaves().map { p =>
        val nodeName = p match {
          case TempView(name, _) => name
          case LogicalRelation(_, _, Some(table), false) => table.qualifiedName
          case HiveTableRelation(table, _, _, _, _) => table.qualifiedName
          case _ => s"${p.nodeName}_${nextNodeId.getAndIncrement()}"
        }
        val outputAttrWithIndex = p.output.zipWithIndex
        val outputAttrs = outputAttrWithIndex.map { case (attr, i) =>
          s"""<tr><td port="$i">${normalizeForHtml(attr.name)}</td></tr>"""
        }
        val nodeInfo =
          s"""
             |"$nodeName" [label=<
             |<table border="1" cellborder="0" cellspacing="0">
             |  <tr><td bgcolor="${nodeColor(p)}"><i>$nodeName</i></td></tr>
             |  ${outputAttrs.mkString("\n")}
             |</table>>];
           """.stripMargin

        (nodeName, nodeInfo, p.output)
      }

      // Collect references between input/output
      val refMap = mutable.HashMap[ExprId, mutable.Set[ExprId]]()
      collectRefsRecursively(optimized, refMap)

      if (!optimized.isInstanceOf[TempView]) {
        val outputAttrs = optimized.output.zipWithIndex.map { case (attr, i) =>
          s"""<tr><td port="$i">${normalizeForHtml(attr.name)}</td></tr>"""
        }

        val tempViewNodeInfo =
          s"""
             |"$tempView" [label=<
             |<table border="1" cellborder="0" cellspacing="0">
             |  <tr><td bgcolor="${nodeColor(TempView(tempView, null))}"><i>$tempView</i></td></tr>
             |  ${outputAttrs.mkString("\n")}
             |</table>>];
           """.stripMargin

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

        val outputAttrMap = optimized.output.map(_.exprId).zipWithIndex.toMap
        val edgeEntries = inputNodes.flatMap { case (inputNodeId, _, input) =>
          input.zipWithIndex.flatMap { case (a, i) =>
            traverseInRefMap(a.exprId).flatMap { attr =>
              if (outputAttrMap.contains(attr)) {
                Some(s""""$inputNodeId":$i -> $tempView:${outputAttrMap(attr)}""")
              } else {
                None
              }
            }
          }
        }

        (inputNodes.map(_._2) :+ tempViewNodeInfo, edgeEntries)
      } else {
        // If a given plan is `TempView t1, [a#102, b#103]`, `nodeName` should be equal to
        // `tempView` and we don't need a new node and edges for `TempView`.
        (inputNodes.map(_._2), Nil)
      }
    }.unzip

    if (nodes.nonEmpty) {
      s"""
         |digraph {
         |  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
         |  node [shape=plain]
         |  rankdir=LR;
         |
         |  ${nodes.flatten.distinct.sorted.mkString("\n")}
         |  ${edges.flatten.distinct.sorted.mkString("\n")}
         |}
       """.stripMargin
    } else {
      ""
    }
  }

  private def collectRefsRecursively(
      plan: LogicalPlan,
      refMap: mutable.HashMap[ExprId, mutable.Set[ExprId]]): Unit = plan match {
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

          case Join(_, _, _, Some(c), _) =>
            c.collect { case BinaryComparison(e1, e2) => (e1, e2) }.foreach { case (e1, e2) =>
              e1.references.foreach { a1 => e2.references.foreach { a2 =>
                addRefsToMap(a1, Seq(a1, a2))
                addRefsToMap(a2, Seq(a1, a2))
              }}
            }

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

        case _ =>
          plan.output
      }
      val missingRefs = (AttributeSet(planOutput) -- plan.producedAttributes).filterNot { a =>
        candidateOutputRefs.contains(a.exprId)
      }
      assert(missingRefs.isEmpty,
        s"""refMap does not have enough entries for ${plan.nodeName}:
           |missingRefs: ${missingRefs.mkString(", ")}
           |${plan.treeString}
           |""".stripMargin)
  }

  private[sql] def catalogToSQLFlow(session: SparkSession): String = {
    val catalog = session.sessionState.catalog
    val tempViewMap = catalog.getTempViewNames().map { tempView =>
      tempView -> catalog.getTempView(tempView).get
    }.toMap

    val (nodes, edges) = catalog.getTempViewNames.map { tempView =>
      val analyzed = session.sessionState.analyzer.execute(tempViewMap(tempView))
      val normalized = analyzed.transformDown {
        case s @ SubqueryAlias(AliasIdentifier(name, Nil), _) if tempViewMap.contains(name) =>
          TempView(name, s.output)
      }
      val optimized = session.sessionState.optimizer.execute(normalized)
      val (nodeName, outputAttrMap, nodeEntries, edgeEntries) = traversePlanRecursively(optimized)

      if (nodeName != tempView) {
        val (outputAttrs, tempViewEdges) = outputAttrMap.zipWithIndex.map {
          case ((attr, input), i) =>
            (s"""<tr><td port="$i">${normalizeForHtml(attr.name)}</td></tr>""",
              s"""$input -> "$tempView":$i;""")
        }.unzip

        // scalastyle:off line.size.limit
        val tempViewNodeInfo =
          s"""
             |"$tempView" [label=<
             |<table border="1" cellborder="0" cellspacing="0">
             |  <tr><td bgcolor="${nodeColor(TempView(tempView, null))}" port="nodeName"><i>$tempView</i></td></tr>
             |  ${outputAttrs.mkString("\n")}
             |</table>>];
           """.stripMargin
        // scalastyle:on line.size.limit

        (nodeEntries :+ tempViewNodeInfo, edgeEntries ++ tempViewEdges)
      } else {
        // If a given plan is `TempView t1, [a#102, b#103]`, `nodeName` should be equal to
        // `tempView` and we don't need a new node and edges for `TempView`.
        (nodeEntries, edgeEntries)
      }
    }.unzip

    if (nodes.nonEmpty) {
      s"""
         |digraph {
         |  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
         |  node [shape=plain]
         |  rankdir=LR;
         |
         |  ${nodes.flatten.distinct.sorted.mkString("\n")}
         |  ${edges.flatten.distinct.sorted.mkString("\n")}
         |}
       """.stripMargin
    } else {
      ""
    }
  }

  private[sql] def planToSQLFlow(plan: LogicalPlan): String = {
    val (_, _, nodeEntries, edgeEntries) = traversePlanRecursively(plan)
    s"""
       |digraph {
       |  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
       |  node [shape=plain]
       |  rankdir=LR;
       |
       |  ${nodeEntries.distinct.sorted.mkString("\n")}
       |  ${edgeEntries.distinct.sorted.mkString("\n")}
       |}
     """.stripMargin
  }

  private def isCached(name: String): Boolean = {
    SparkSession.getActiveSession.exists { session =>
      session.sessionState.catalog.getTempView(name).exists { p =>
        val analyzed = session.sessionState.analyzer.execute(p)
        session.sharedState.cacheManager.lookupCachedData(analyzed).isDefined
      }
    }
  }

  private def normalizeForHtml(str: String) = {
    str.replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
  }

  private def nodeColor(plan: LogicalPlan): String = plan match {
    case TempView(name, _) if isCached(name) => "lightblue"
    case _: TempView => "lightyellow"
    case _: LeafNode => "lightpink"
    case _ => "lightgray"
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

  private def traversePlanRecursively(plan: LogicalPlan)
    : (String, Seq[(Attribute, String)], Seq[String], Seq[String]) = plan match {
    case _: LeafNode =>
      val nodeName = plan match {
        case TempView(name, _) => name
        case LogicalRelation(_, _, Some(table), false) => table.qualifiedName
        case HiveTableRelation(table, _, _, _, _) => table.qualifiedName
        case _ => s"${plan.nodeName}_${nextNodeId.getAndIncrement()}"
      }
      val outputAttrWithIndex = plan.output.zipWithIndex
      val (outputAttrs, outputAttrMap) = outputAttrWithIndex.map { case (attr, i) =>
        (s"""<tr><td port="$i">${normalizeForHtml(attr.name)}</td></tr>""",
          attr -> s""""$nodeName":$i""")
      }.unzip
      // scalastyle:off line.size.limit
      val nodeInfo =
        s"""
           |"$nodeName" [label=<
           |<table border="1" cellborder="0" cellspacing="0">
           |  <tr><td bgcolor="${nodeColor(plan)}" port="nodeName"><i>$nodeName</i></td></tr>
           |  ${outputAttrs.mkString("\n")}
           |</table>>];
       """.stripMargin
      // scalastyle:on line.size.limit

      (nodeName, outputAttrMap, Seq(nodeInfo), Nil)

    case _ =>
      val inputInfos = plan.children.map(traversePlanRecursively)
      val nodeId = nextNodeId.getAndIncrement()
      val nodeName = plan match {
        case j: Join => s"${plan.nodeName}_${j.joinType}_$nodeId"
        case _ => s"${plan.nodeName}_$nodeId"
      }

      val subqueries = plan.expressions.flatMap(_.collect { case ss: ScalarSubquery => ss })
      val (nodesInSubquries, edgesInSubqueries) = if (subqueries.nonEmpty) {
        val (n, e) = subqueries.map { ss =>
          val (_, outputAttrMap, n, e) = traversePlanRecursively(ss.plan)
          val edges = e ++ outputAttrMap.map { case (_, src) =>
            s"""$src -> "$nodeName":nodeName"""
          }
          (n, edges)
        }.unzip

        (n.flatten, e.flatten)
      } else {
        (Nil, Nil)
      }

      if (plan.output.nonEmpty) {
        val outputAttrsWithIndex = plan.output.zipWithIndex
        val (outputAttrs, outputAttrMap) = outputAttrsWithIndex.map { case (attr, i) =>
          (s"""<tr><td port="$i">${normalizeForHtml(attr.name)}</td></tr>""",
            attr -> s""""$nodeName":$i""")
        }.unzip
        val edgeInfo = collectEdges(nodeName, plan, inputInfos.map(_._2), outputAttrsWithIndex)
        // scalastyle:off line.size.limit
        val nodeInfo =
          s"""
             |"$nodeName" [label=<
             |<table border="1" cellborder="0" cellspacing="0">
             |  <tr><td bgcolor="${nodeColor(plan)}" port="nodeName"><i>$nodeName</i></td></tr>
             |  ${outputAttrs.mkString("\n")}
             |</table>>];
         """.stripMargin
        // scalastyle:on line.size.limit

        (nodeName, outputAttrMap, (nodeInfo +: inputInfos.flatMap(_._3)) ++ nodesInSubquries,
          edgeInfo ++ inputInfos.flatMap(_._4) ++ edgesInSubqueries)
      } else {
        (nodeName, Nil, inputInfos.flatMap(_._3) ++ nodesInSubquries,
          inputInfos.flatMap(_._4) ++ edgesInSubqueries)
      }
  }
}
