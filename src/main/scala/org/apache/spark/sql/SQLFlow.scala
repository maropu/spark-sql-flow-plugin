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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeSet, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.LeftExistence
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class SQLFlowHolder[T] private[sql](private val ds: Dataset[T]) {

  def displayFlow(): Unit = {
    // scalastyle:off println
    println(SQLFlow.planToSQLFlow(ds.queryExecution.optimizedPlan))
    // scalastyle:on println
  }
}

case class TempView(name: String, output: Seq[Attribute]) extends LeafNode {
  override lazy val resolved: Boolean = true
}

object SQLFlow extends PredicateHelper {

  private val nextNodeId = new AtomicInteger(0)

  implicit def DatasetToSQLFlowHolder[T](ds: Dataset[T]): SQLFlowHolder[T] = {
    new SQLFlowHolder[T](ds)
  }

  def printCatalogAsSQLFlow(): Unit = {
    // scalastyle:off println
    println(catalogToSQLFlow().getOrElse(""))
    // scalastyle:on println
  }

  private[sql] def catalogToSQLFlow(): Option[String] = {
    SparkSession.getActiveSession.map { session =>
      val catalog = session.sessionState.catalog
      val tempViewMap = catalog.getTempViewNames().map { tempView =>
        tempView -> catalog.getTempView(tempView).get
      }.toMap

      val tempViewFlowMap = mutable.Map[String, (String, Seq[(Attribute, String)], LogicalPlan)]()

      val (nodes, edges) = catalog.getTempViewNames.map { tempView =>
        val analyzed = session.sessionState.analyzer.execute(tempViewMap(tempView))
        val normalized = analyzed.transformDown {
          case s @ SubqueryAlias(AliasIdentifier(name, Nil), _) if tempViewMap.contains(name) =>
            TempView(name, s.output)
        }
        val optimized = session.sessionState.optimizer.execute(normalized)
        val (nodeName, outputAttrMap, nodeEntries, edgeEntries) = parsePlanRecursively(optimized)

        if (nodeName != tempView) {
          val (outputAttrs, tempViewEdges) = outputAttrMap.zipWithIndex.map {
            case ((attr, input), i) =>
              (s"""<tr><td port="$i">${attr.name}</td></tr>""", s"""$input -> "$tempView":$i;""")
          }.unzip

          // scalastyle:off line.size.limit
          val tempViewNodeInfo =
            s"""
               |"$tempView" [label=<
               |<table border="1" cellborder="0" cellspacing="0">
               |  <tr><td bgcolor="${nodeColor(TempView(tempView, null))}"><i>$tempView</i></td></tr>
               |  ${outputAttrs.mkString("\n")}
               |</table>>];
             """.stripMargin
          // scalastyle:on line.size.limit

          tempViewFlowMap(tempView) = (nodeName, outputAttrMap, optimized)
          (nodeEntries :+ tempViewNodeInfo, edgeEntries ++ tempViewEdges)
        } else {
          // If a given plan is `TempView t1, [a#102, b#103]`, `nodeName` should be equal to
          // `tempView` and we don't need a new node and edges for `TempView`.
          (nodeEntries, edgeEntries)
        }
      }.unzip

      s"""
         |digraph {
         |  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
         |  node [shape=plain]
         |  rankdir=LR;
         |
         |  ${nodes.flatten.sorted.mkString("\n")}
         |  ${edges.flatten.sorted.mkString("\n")}
         |}
       """.stripMargin
    }
  }

  private[sql] def planToSQLFlow(plan: LogicalPlan): String = {
    val (_, _, nodeEntries, edgeEntries) = parsePlanRecursively(plan)
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

  private def nodeColor(plan: LogicalPlan): String = plan match {
    case TempView(name, _) if isCached(name) => "lightblue"
    case _: TempView => "lightyellow"
    case _: LeafNode => "lightpink"
    case _ => "lightgray"
  }

  private def getEdeges(
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

  private def parsePlanRecursively(plan: LogicalPlan)
    : (String, Seq[(Attribute, String)], Seq[String], Seq[String]) = plan match {
    case _: LeafNode =>
      val nodeId = nextNodeId.getAndIncrement()
      val nodeName = plan match {
        case TempView(name, _) => name
        case LogicalRelation(_, _, Some(table), false) => table.qualifiedName
        case HiveTableRelation(table, _, _, _, _) => table.qualifiedName
        case _ => s"${plan.nodeName}_$nodeId"
      }
      val outputAttrWithIndex = plan.output.zipWithIndex
      val (outputAttrs, outputAttrMap) = outputAttrWithIndex.map { case (attr, i) =>
        (s"""<tr><td port="$i">${attr.name}</td></tr>""", attr -> s""""$nodeName":$i""")
      }.unzip
      val nodeInfo =
        s"""
           |"$nodeName" [label=<
           |<table border="1" cellborder="0" cellspacing="0">
           |  <tr><td bgcolor="${nodeColor(plan)}"><i>$nodeName</i></td></tr>
           |  ${outputAttrs.mkString("\n")}
           |</table>>];
       """.stripMargin

      (nodeName, outputAttrMap, Seq(nodeInfo), Nil)

    case _ =>
      val inputInfos = plan.children.map(parsePlanRecursively)
      val nodeId = nextNodeId.getAndIncrement()
      val nodeName = plan match {
        case j: Join => s"${plan.nodeName}_${j.joinType}_$nodeId"
        case _ => s"${plan.nodeName}_$nodeId"
      }
      if (plan.output.nonEmpty) {
        val inputInfos = plan.children.map(parsePlanRecursively)

        val outputAttrsWithIndex = plan.output.zipWithIndex
        val (outputAttrs, outputAttrMap) = outputAttrsWithIndex.map { case (attr, i) =>
          (s"""<tr><td port="$i">${attr.name}</td></tr>""", attr -> s""""$nodeName":$i""")
        }.unzip
        val edgeInfo = getEdeges(nodeName, plan, inputInfos.map(_._2), outputAttrsWithIndex)
        val nodeInfo =
          s"""
             |"$nodeName" [label=<
             |<table border="1" cellborder="0" cellspacing="0">
             |  <tr><td bgcolor="${nodeColor(plan)}"><i>$nodeName</i></td></tr>
             |  ${outputAttrs.mkString("\n")}
             |</table>>];
         """.stripMargin

        (nodeName, outputAttrMap, nodeInfo +: inputInfos.flatMap(_._3),
          edgeInfo ++ inputInfos.flatMap(_._4))
      } else {
        (nodeName, Nil, inputInfos.flatMap(_._3), inputInfos.flatMap(_._4))
      }
  }
}
