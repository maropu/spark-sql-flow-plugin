
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

object GraphNodeType extends Enumeration {
  val TableNode, PlanNode = Value
}

case class SQLFlowGraphNode(
  ident: String,
  attributes: Seq[String],
  tpe: GraphNodeType.Value,
  isCached: Boolean) {

  private def prettyTypeName(tpe: GraphNodeType.Value) = tpe match {
    case GraphNodeType.TableNode => "table"
    case GraphNodeType.PlanNode => "plan"
  }

  override def toString: String = {
    s"""name=`$ident`(${attributes.map(a => s"`$a`").mkString(",")}), """ +
      s"""type=${prettyTypeName(tpe)}, cached=$isCached"""
  }
}

case class SQLFlowGraphEdge(
  from: String,
  fromIdx: Option[Int],
  to: String,
  toIdx: Option[Int]) {

  override def toString: String = {
    val fromIdxOpt = fromIdx.map(i => s"(idx=$i)").getOrElse("")
    val toIdxOpt = toIdx.map(i => s"(idx=$i)").getOrElse("")
    s"""from=`$from`$fromIdxOpt, to=`$to`$toIdxOpt"""
  }
}

abstract class BaseGraphFormat {
  def toGraphString(nodes: Seq[SQLFlowGraphNode], edges: Seq[SQLFlowGraphEdge]): String
}