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

package org.apache.spark.api.python

import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql.flow.{AdjacencyListFormat, BaseGraphFormat, GraphFileWriter, GraphVizFormat, SQLFlow}
import org.apache.spark.util.{Utils => SparkUtils}

object SQLFlowApi extends Logging {

  private def parseOptions(options: String): Map[String, String] = {
    SparkUtils.stringToSeq(options).flatMap { kv =>
      kv.split("=").toSeq match {
        case Seq(k, v) => Some((k, v))
        case s =>
          logWarning(s"Unknown option format: $s")
          None
      }
    }.toMap
  }

  private def toGraphFormat(fmt: String, options: Map[String, String])
    : BaseGraphFormat with GraphFileWriter = {
    fmt.toLowerCase(Locale.ROOT) match {
      case "graphviz" =>
        val imgFormat = options.getOrElse("imgFormat", "svg")
        GraphVizFormat(imgFormat)

      case "adjacency_list" =>
        val sepString = options.getOrElse("sep", ",")
        AdjacencyListFormat(sepString)

      case _ =>
        throw new IllegalStateException(s"Unknown graph format: $fmt")
    }
  }

  def debugPrintAsSQLFlow(
      contracted: Boolean,
      graphFormat: String = "graphviz",
      options: String = ""): Unit = {
    val graphFmt = toGraphFormat(graphFormat, parseOptions(options))
    SQLFlow.printAsSQLFlow(contracted, graphFmt)
  }

  def toSQLFlowString(
      contracted: Boolean,
      graphFormat: String = "graphviz",
      options: String = ""): String = {
    val graphFmt = toGraphFormat(graphFormat, parseOptions(options))
    SQLFlow.toSQLFlowString(contracted, graphFormat = graphFmt)
  }

  def saveAsSQLFlow(
      path: String,
      filenamePrefix: String,
      graphFormat: String = "graphviz",
      contracted: Boolean,
      overwrite: Boolean,
      options: String = ""): Unit = {
    val graphFmt = toGraphFormat(graphFormat, parseOptions(options))
    SQLFlow.saveAsSQLFlow(path, filenamePrefix, graphFmt, contracted, overwrite)
  }
}
