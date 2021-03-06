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

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.flow._
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class Neo4jAuraSinkSuite extends QueryTest with SharedSparkSession
  with SQLTestUtils with SQLFlowTestUtils with Neo4jAuraTest {

  test("neo4j write/read") {
    val sink = Neo4jAuraSink(uri, user, passwd)
    val nodes = Seq("nodeA", "nodeB", "nodeC").map { ident =>
      SQLFlowGraphNode(ident, ident, Seq("c"), "c INT", GraphNodeType.TableNode, false)
    }
    val edges = Seq(
      SQLFlowGraphEdge("nodeA", None, "nodeB", None),
      SQLFlowGraphEdge("nodeB", None, "nodeC", None)
    )
    sink.write(nodes, edges, Map.empty)

    withSession { s =>
      withTx(s) { tx =>
        val r = tx.run(s"""
             |MATCH (from:Table { name: "nodeA" })-[:transformInto*2]->(to:Table { name: "nodeC"})
             |RETURN count(to) AS cnt
           """.stripMargin)
        assert(r.single().get("cnt").asInt === 1)
      }
    }
  }

  private def checkNodeCount(expected: Int): Unit = {
    withSession { s =>
      withTx(s) { tx =>
        val r = tx.run("MATCH (n) RETURN count(*)").single()
        assert(r.get(0).asInt === expected)
      }
    }
  }

  test("neo4j write/read - stream") {
    withListener(SQLFlowListener(Neo4jAuraSink(uri, user, passwd))) {
      val df1 = spark.range(1).selectExpr("id as k", "id as v")
      checkAnswer(df1, Row(0, 0) :: Nil)
      spark.sparkContext.listenerBus.waitUntilEmpty()
      checkNodeCount(3)

      val df2 = spark.range(1)
        .selectExpr("id as k", "id as v")
        .groupBy("k")
        .count()
      checkAnswer(df2, Row(0, 1) :: Nil)
      spark.sparkContext.listenerBus.waitUntilEmpty()
      checkNodeCount(6)

      val df3 = spark.range(1)
        .selectExpr("id as k", "id as v")
        .groupBy("k")
        .count()
        .where("count = 1")
      checkAnswer(df3, Row(0, 1) :: Nil)
      spark.sparkContext.listenerBus.waitUntilEmpty()
      checkNodeCount(8)

      withSession { s =>
        withTx(s) { tx =>
          val r1 = tx.run(s"""
             |MATCH (from:LeafPlan { name: "Range" })-[:transformInto*1..]->(to:Query)
             |RETURN count(to) AS cnt
           """.stripMargin)
          assert(r1.single().get("cnt").asInt === 3)

          val r2 = tx.run(s"""
             |MATCH (s)-[t:transformInto]->(e)
             |RETURN s.name AS sn, size(t.dstNodeIds) AS cnt
           """.stripMargin)
          val edges = r2.asScala.map { r =>
            (r.get("sn").asString, r.get("cnt").asInt)
          }
          assert(edges.toSet === Set(
            ("Project", 2),
            ("Range", 1),
            ("Aggregate", 1),
            ("Project", 1),
            ("Range", 2),
            ("Filter", 1),
            ("Aggregate", 1)))
        }
      }
    }
  }

  test("semantically-equal plan node merging - stream") {
    withListener(SQLFlowListener(Neo4jAuraSink(uri, user, passwd))) {
      val df1 = spark.range(1)
        .selectExpr("id as k", "id as v")
        .groupBy("k")
        .count()
      checkAnswer(df1, Row(0, 1) :: Nil)
      spark.sparkContext.listenerBus.waitUntilEmpty()
      checkNodeCount(4)

      val df2 = spark.range(1)
        .selectExpr("id as k", "id as v")
        .groupBy("k")
        .count().as("count")
        .where("count = 1")
      checkAnswer(df2, Row(0, 1) :: Nil)
      spark.sparkContext.listenerBus.waitUntilEmpty()
      checkNodeCount(6)

      withSession { s =>
        withTx(s) { tx =>
          val queryNodeNames = tx.run("MATCH(n:Query) RETURN n.name AS name").asScala
            .map { r => r.get("name").asString }.toSeq
          assert(queryNodeNames.size === 2)

          val Seq(sHashValueSet1, sHashValueSet2) = queryNodeNames.map { nodeName =>
            tx.run(s"""
                 |MATCH p=(n:Plan)-[:transformInto*1..]->(to:Query {name: "$nodeName"})
                 |RETURN n.semanticHash AS semanticHash
               """.stripMargin
            ).asScala.map { r =>
              r.get("semanticHash").asString
            }.toSet
          }
          // We assume the three nodes are overlapped in data lineage
          assert((sHashValueSet1 & sHashValueSet2).size === 2)
        }
      }
    }
  }

  test("overwrite option") {
    val sink = Neo4jAuraSink(uri, user, passwd)
    val nodeType = GraphNodeType.TableNode
    val nodes = SQLFlowGraphNode("nodeA", "nodeA", Seq("c"), "c INT", nodeType, false) :: Nil
    sink.write(nodes, Nil, Map.empty)

    val errMsg = intercept[SparkException] {
      sink.write(nodes, Nil, Map.empty)
    }.getMessage
    assert(errMsg === "Failed to execution tx because: Database should be empty")

    sink.write(nodes, Nil, Map("overwrite" -> "true"))
    withSession { s =>
      withTx(s) { tx =>
        val names = tx.run("MATCH (n) RETURN n.name AS name").asScala
          .map { r => r.get("name").asString }.toSeq
        assert(names === Seq("nodeA"))
      }
    }
  }
}
