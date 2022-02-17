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

package org.apache.spark.sql.flow.sink

import java.io.File

import org.apache.spark.sql.flow.{GraphNodeType, SQLFlowGraphEdge, SQLFlowGraphNode}

/**
 * This class transforms an input graph into a Mermaid-formatted flowchart.
 *  - https://mermaid-js.github.io/mermaid/#/flowchart
 */
case class MermaidSink(imgFormat: String = "svg") extends GraphFileBatchSink {

  override def filenameSuffix: String = "mmd"

  private val className = {
    getClass.getCanonicalName
  }

  private def tryGenerateImageFile(options: Map[String, String]): Unit = {
    val dirPath = getOutputDirPathFrom(options)
    val filenamePrefix = getFilenamePrefixFrom(options)
    val mmdFile = new File(dirPath, s"$filenamePrefix.$filenameSuffix").getAbsolutePath
    val dstFile = new File(dirPath, s"$filenamePrefix.$imgFormat").getAbsolutePath
    val arguments = s"-i $mmdFile -o $dstFile"
    SinkUtils.tryToExecuteCommand("mmdc", arguments)
  }

  override def write(
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Unit = {
    super.write(nodes, edges, options)
    tryGenerateImageFile(options)
  }

  override def toGraphString(nodes: Seq[SQLFlowGraphNode], edges: Seq[SQLFlowGraphEdge]): String = {
    val nodeStrings = nodes.map { n =>
      val nodeDesc = n.tpe match {
        case GraphNodeType.TableNode | GraphNodeType.ViewNode | GraphNodeType.QueryNode =>
          val desc = s""""${n.ident}(${n.attributeNames.mkString(",")})""""
          s"[[$desc]]"
        case _ =>
          s"(${n.ident})"
      }
      s"    ${n.uniqueId}$nodeDesc"
    }
    val compactEdges = edges.map { e => (e.fromId, e.toId) }.distinct
    val edgeStrings = compactEdges.map { case (fromId, toId) =>
      s"""    $fromId-->$toId"""
    }
    s"""
       |%% Automatically generated by $className
       |flowchart LR
       |${nodeStrings.mkString("\n")}
       |${edgeStrings.mkString("\n")}
     """.stripMargin
  }
}
