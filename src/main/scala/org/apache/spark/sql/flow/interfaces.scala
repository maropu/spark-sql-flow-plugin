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

import scala.collection.mutable

object GraphNodeType extends Enumeration {
  val TableNode, ViewNode, PlanNode, LeafPlanNode, QueryNode = Value
}

case class SQLFlowGraphNode(
  uniqueId: String,
  ident: String,
  attributeNames: Seq[String],
  schemaDDL: String,
  tpe: GraphNodeType.Value,
  isCached: Boolean,
  props: mutable.Map[String, String] = mutable.Map.empty) {

  private def prettyTypeName(tpe: GraphNodeType.Value) = tpe match {
    case GraphNodeType.TableNode => "table"
    case GraphNodeType.ViewNode => "view"
    case GraphNodeType.PlanNode => "plan"
    case GraphNodeType.LeafPlanNode => "leaf_plan"
    case GraphNodeType.QueryNode => "query"
  }

  override def toString: String = {
    s"""name=`$ident`(${attributeNames.map(a => s"`$a`").mkString(",")}), """ +
      s"""type=${prettyTypeName(tpe)}, cached=$isCached"""
  }
}

case class SQLFlowGraphEdge(
  fromId: String,
  fromIdx: Option[Int],
  toId: String,
  toIdx: Option[Int]) {

  override def toString: String = {
    val fromIdxOpt = fromIdx.map(i => s"(idx=$i)").getOrElse("")
    val toIdxOpt = toIdx.map(i => s"(idx=$i)").getOrElse("")
    s"""from=`$fromId`$fromIdxOpt, to=`$toId`$toIdxOpt"""
  }
}

trait BaseGraphBatchSink {
  def write(nodes: Seq[SQLFlowGraphNode], edges: Seq[SQLFlowGraphEdge],
    options: Map[String, String]): Unit
}

trait BaseGraphStreamSink {
  def append(nodes: Seq[SQLFlowGraphNode], edges: Seq[SQLFlowGraphEdge],
    options: Map[String, String]): Unit
}