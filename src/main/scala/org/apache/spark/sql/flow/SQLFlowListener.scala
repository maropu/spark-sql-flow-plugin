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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.util.QueryExecutionListener

case class SQLFlowListener(graphSink: BaseGraphStreamSink, contracted: Boolean = false)
  extends QueryExecutionListener with Logging {

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = try {
    val optimized = qe.optimizedPlan
    optimized match {
      case _: RunnableCommand => // skip
      case _ =>
        val sqlFlow = if (contracted) SQLContractedFlow() else SQLFlow()
        val (nodes, edges) = sqlFlow.planToSQLFlow(optimized)
        graphSink.append(nodes, edges, Map.empty[String, String])
    }
  } catch {
    case NonFatal(e) =>
      logWarning(s"Failed to append data lineage because: ${e.getMessage}")
  }
}