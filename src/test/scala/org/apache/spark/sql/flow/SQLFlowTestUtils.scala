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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.QueryExecutionListener

trait SQLFlowTestUtils {

  protected def withListener(listener: QueryExecutionListener)(f: => Unit): Unit = {
    val spark = SparkSession.getActiveSession.getOrElse {
      throw new IllegalStateException("Active SparkSession not found")
    }
    try {
      spark.listenerManager.register(listener)
      f
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  protected def getOutputAsString(f: => Unit): String = {
    val output = new java.io.ByteArrayOutputStream()
    Console.withOut(output) { f }
    output.toString
  }

  private def normalize(s: String): String = {
    s.replaceAll("_[a-z0-9]{7}", "_x")
      .replaceAll("_[0-9]{1,2}", "_x")
  }

  private def extractEdgesFrom(s: String, edgeRegex: String): Set[String] = {
    edgeRegex.r.findAllIn(normalize(s)).toList.toSet
  }

  protected def checkOutputString(edgeRegex: String)(actual: String, expected: String): Unit = {
    val expectedEdges = extractEdgesFrom(expected, edgeRegex)
    assert(expectedEdges.nonEmpty && extractEdgesFrom(actual, edgeRegex) == expectedEdges,
      s"`$actual` didn't match an expected string `$expected`")
  }
}
