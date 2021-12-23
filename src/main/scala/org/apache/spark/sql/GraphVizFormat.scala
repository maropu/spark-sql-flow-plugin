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

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.immutable.Stream
import scala.sys.process._
import scala.util.Try

import org.apache.spark.internal.Logging

/**
 * Using ProcessBuilder.lineStream produces a stream, that uses
 * a LinkedBlockingQueue with a default capacity of Integer.MAX_VALUE.
 *
 * This causes OOM if the consumer cannot keep up with the producer.
 *
 * See scala.sys.process.ProcessBuilderImpl.lineStream
 */
private[sql] object BlockingLineStream {

  // See scala.sys.process.Streamed
  private final class BlockingStreamed[T](
    val process: T => Unit,
    val done: Int => Unit,
    val stream: () => Stream[T])

  // See scala.sys.process.Streamed
  private object BlockingStreamed {
    // scala.process.sys.Streamed uses default of Integer.MAX_VALUE,
    // which causes OOMs if the consumer cannot keep up with producer.
    val maxQueueSize = 65536

    def apply[T](nonzeroException: Boolean): BlockingStreamed[T] = {
      val q = new LinkedBlockingQueue[Either[Int, T]](maxQueueSize)

      def next(): Stream[T] = q.take match {
        case Left(0) => Stream.empty
        case Left(code) =>
          if (nonzeroException) scala.sys.error("Nonzero exit code: " + code) else Stream.empty
        case Right(s) => Stream.cons(s, next())
      }

      new BlockingStreamed((s: T) => q put Right(s), code => q put Left(code), () => next())
    }
  }

  // See scala.sys.process.ProcessImpl.Spawn
  private object Spawn {
    def apply(f: => Unit): Thread = apply(f, daemon = false)

    def apply(f: => Unit, daemon: Boolean): Thread = {
      val thread = new Thread() {
        override def run() = {
          f
        }
      }
      thread.setDaemon(daemon)
      thread.start()
      thread
    }
  }

  def apply(command: Seq[String]): Stream[String] = {
    val streamed = BlockingStreamed[String](true)
    val process = command.run(BasicIO(false, streamed.process, None))
    Spawn(streamed.done(process.exitValue()))
    streamed.stream()
  }
}

// TODO: Supports more formats to export data lineage into other systems,
// e.g., Apache Atlas, neo4j, ...
case object GraphVizFormat extends BaseGraphFormat with Logging {

  private val cachedNodeColor = "lightblue"

  override def toGraphString(nodes: Seq[SQLFlowGraphNode], edges: Seq[SQLFlowGraphEdge]): String = {
    if (nodes.nonEmpty) {
      val nodeStrings = nodes.map(generateNodeString)
      val edgeStrings = edges.map(generateEdgeString)
      s"""
         |digraph {
         |  graph [pad="0.5" nodesep="0.5" ranksep="1" fontname="Helvetica" rankdir=LR];
         |  node [shape=plaintext]
         |
         |  ${nodeStrings.sorted.mkString("\n")}
         |  ${edgeStrings.sorted.mkString("\n")}
         |}
       """.stripMargin
    } else {
      ""
    }
  }

  private def generateNodeString(node: SQLFlowGraphNode): String = {
    node.tpe match {
      case GraphNodeType.TableNode => generateTableNodeString(node)
      case GraphNodeType.PlanNode => generatePlanNodeString(node)
    }
  }

  private def generateEdgeString(edge: SQLFlowGraphEdge): String = {
    val toIdxStr = (i: Option[Int]) => i.map(_.toString).getOrElse("nodeName")
    s""""${edge.from}":${toIdxStr(edge.fromIdx)} -> "${edge.to}":${toIdxStr(edge.toIdx)};"""
  }

  private def generateTableNodeString(node: SQLFlowGraphNode): String = {
    val nodeColor = if (node.isCached) cachedNodeColor else "black"
    val outputAttrs = node.attributes.zipWithIndex.map { case (attr, i) =>
      s"""<tr><td port="$i">${normalizeForHtml(attr)}</td></tr>"""
    }
    // scalastyle:off line.size.limit
    s"""
       |"${node.ident}" [color="$nodeColor" label=<
       |<table>
       |  <tr><td bgcolor="$nodeColor" port="nodeName"><i><font color="white">${node.ident}</font></i></td></tr>
       |  ${outputAttrs.mkString("\n")}
       |</table>>];
     """.stripMargin
    // scalastyle:on line.size.limit
  }

  private def generatePlanNodeString(node: SQLFlowGraphNode): String = {
    val nodeColor = if (node.isCached) cachedNodeColor else "lightgray"
    val outputAttrs = node.attributes.zipWithIndex.map { case (attr, i) =>
      s"""<tr><td port="$i">${normalizeForHtml(attr)}</td></tr>"""
    }
    // scalastyle:off line.size.limit
    s"""
       |"${node.ident}" [label=<
       |<table color="$nodeColor" border="1" cellborder="0" cellspacing="0">
       |  <tr><td bgcolor="$nodeColor" port="nodeName"><i>${node.ident}</i></td></tr>
       |  ${outputAttrs.mkString("\n")}
       |</table>>];
     """.stripMargin
    // scalastyle:on line.size.limit
  }

  private def normalizeForHtml(str: String): String = {
    str.replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
  }

  private def isCommandAvailable(command: String): Boolean = {
    val attempt = {
      Try(Process(Seq("sh", "-c", s"command -v $command")).run(ProcessLogger(_ => ())).exitValue())
    }
    attempt.isSuccess && attempt.get == 0
  }

  // If the Graphviz dot command installed, converts the generated dot file
  // into a specified-formatted image.
  private[sql] def tryGenerateImageFile(format: String, src: String, dst: String): Unit = {
    if (isCommandAvailable("dot")) {
      try {
        val commands = Seq("bash", "-c", s"dot -T$format $src > $dst")
        BlockingLineStream(commands)
      } catch {
        case e =>
          logWarning(s"Failed to generate a graph image (fmt=$format): ${e.getMessage}")
      }
    }
  }
}