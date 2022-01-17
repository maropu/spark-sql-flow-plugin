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

import java.time.Instant

import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.flow.sink.Neo4jAuraSink
import org.apache.spark.sql.util.QueryExecutionListener

abstract class BaseSQLFlowListener extends QueryExecutionListener with Logging {
  def graphSink: BaseGraphStreamSink
  def contracted: Boolean

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

  private def setPropsInDstNode(
      nodes: Seq[SQLFlowGraphNode],
      dstNodeName: String,
      props: Map[String, String]): Unit = {
    nodes.find(_.ident == dstNodeName).map { dstNode =>
      dstNode.props ++= props
    }.getOrElse {
      logWarning(s"Query output node '$dstNodeName' not found in data lineage")
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = try {
    val optimized = qe.optimizedPlan
    optimized match {
      case _: Command =>
        // TODO: Tracks data lineage between table/views via INSERT queries (Issue#5)
      case _ =>
        val sqlFlow = if (contracted) SQLContractedFlow() else SQLFlow()
        val dstNodeName = s"query_${Math.abs(qe.hashCode)}"
        val (nodes, edges) = sqlFlow.planToSQLFlow(optimized, Some(dstNodeName))
        setPropsInDstNode(nodes, dstNodeName, Map(
          "durationMs" -> s"${durationNs / (1000 * 1000)}",
          "timestamp" -> s"${Instant.now()}"
        ))
        graphSink.append(nodes, edges, Map.empty)
    }
  } catch {
    case NonFatal(e) =>
      logWarning(s"Failed to append data lineage because: ${e.getMessage}")
  }
}

case class SQLFlowListener(graphSink: BaseGraphStreamSink, contracted: Boolean = false)
  extends BaseSQLFlowListener

case class Neo4jAuraSQLFlowListener(conf: SparkConf) extends BaseSQLFlowListener {
  private val configPrefix = s"spark.sql.flow.${classOf[Neo4jAuraSink].getSimpleName}"
  override val graphSink: BaseGraphStreamSink = {
    def getConf(key: String) = {
      if (!conf.contains(key)) {
        throw new SparkException(s"To load ${this.getClass.getSimpleName}, " +
          s"`$key` needs to be specified")
      } else {
        conf.get(key)
      }
    }
    val uri = getConf(s"$configPrefix.uri")
    val user = getConf(s"$configPrefix.user")
    val password = getConf(s"$configPrefix.password")
    Neo4jAuraSink(uri, user, password)
  }
  override val contracted: Boolean =
    conf.getBoolean(s"$configPrefix.contracted", false)
}