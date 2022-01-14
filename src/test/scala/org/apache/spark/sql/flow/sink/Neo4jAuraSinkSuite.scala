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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.flow._

class Neo4jAuraSinkSuite extends SparkFunSuite
  with SQLFlowTestUtils with Neo4jAura {

  val uri = System.getenv("NEO4J_AURADB_URI")
  val user = System.getenv("NEO4J_AURADB_USER")
  val passwd = System.getenv("NEO4J_AURADB_PASSWD")

  private lazy val runTests = {
    uri != null && user != null && passwd != null
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    if (runTests) {
      withSession { s =>
        withTx(s) { tx =>
          tx.run("MATCH (n) DETACH DELETE n")
        }
      }
    }
  }

  protected override def test(testName: String, testTags: Tag*)
      (testFun: => Any)(implicit pos: Position): Unit = {
    super.test(testName) {
      assume(runTests)
      testFun
    }
  }

  test("neo4j write/read") {
    val sink = Neo4jAuraSink(uri, user, passwd)
    val nodes = Seq("nodeA", "nodeB", "nodeC").map { ident =>
      SQLFlowGraphNode(ident, Seq("c"), GraphNodeType.TableNode, false)
    }
    val edges = Seq(
      SQLFlowGraphEdge("nodeA", None, "nodeB", None),
      SQLFlowGraphEdge("nodeB", None, "nodeC", None)
    )
    sink.write(nodes, edges, Map.empty)

    withSession { s =>
      withTx(s) { tx =>
        val r = tx.run(s"""
             |MATCH p=(from:Table { name: "nodeA" })-[:transformInto*2]->(to:Table { name: "nodeC"})
             |RETURN p
           """.stripMargin)
        assert(r.keys().size === 1)
      }
    }
  }

  test("Database should be empty") {
    val sink = Neo4jAuraSink(uri, user, passwd)
    val nodes = SQLFlowGraphNode("nodeA", Seq("c"), GraphNodeType.TableNode, false) :: Nil
    sink.write(nodes, Nil, Map.empty)

    val errMsg = intercept[SparkException] {
      val sink = Neo4jAuraSink(uri, user, passwd)
      sink.write(nodes, Nil, Map.empty)
    }.getMessage
    assert(errMsg === "Failed to execution tx because: Database should be empty")
  }
}
