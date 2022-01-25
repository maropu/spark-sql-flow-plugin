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

import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfterEach, Tag}
import org.scalatest.funsuite.AnyFunSuiteLike

import org.apache.spark.sql.flow.sink.Neo4jAura

trait Neo4jAuraTest extends Neo4jAura with AnyFunSuiteLike with BeforeAndAfterEach {

  val uri = System.getenv("NEO4J_AURADB_URI")
  val user = System.getenv("NEO4J_AURADB_USER")
  val passwd = System.getenv("NEO4J_AURADB_PASSWD")

  private lazy val runTests = {
    uri != null && user != null && passwd != null
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    if (runTests) {
      resetNeo4jDbState()
    }
  }

  protected override def test(testName: String, testTags: Tag*)
      (testFun: => Any)(implicit pos: Position): Unit = {
    super.test(testName) {
      assume(runTests)
      testFun
    }
  }
}
