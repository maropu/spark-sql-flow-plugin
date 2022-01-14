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

trait SQLFlowTestUtils {

  protected def getOutputAsString(f: => Unit): String = {
    val output = new java.io.ByteArrayOutputStream()
    Console.withOut(output) { f }
    output.toString
  }

  protected def checkOutputString(actual: String, expected: String): Unit = {
    def normalize(s: String) = {
      s.replaceAll("_\\d+", "_x")
    }
    def extractEdges(s: String): Set[String] = {
      val re = """"[a-zA-Z_]+":\d -> "[a-zA-Z_]+":\d""".r
      re.findAllIn(normalize(s)).toList.toSet
    }
    val expectedEdges = extractEdges(expected)
    assert(expectedEdges.nonEmpty && extractEdges(actual) == expectedEdges,
      s"`$actual` didn't match an expected string `$expected`")
  }
}
