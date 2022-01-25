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
import scala.util.Try
import scala.util.control.NonFatal

import org.neo4j.driver._

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.flow.{BaseGraphBatchSink, BaseGraphStreamSink, GraphNodeType, SQLFlowGraphEdge, SQLFlowGraphNode}

trait Neo4jAura {
  def uri: String
  def user: String
  def passwd: String

  protected def withSession(f: Session => Unit): Unit = {
    var driver: Driver = null
    var session: Session = null
    try {
      driver = GraphDatabase.driver(uri, AuthTokens.basic(user, passwd), Config.defaultConfig())
      session = driver.session()
      f(session)
    } finally {
      if (session != null) {
        session.close()
      }
      if (driver != null) {
        driver.close()
      }
    }
  }

  protected def withTx(s: Session)(f: Transaction => Unit): Unit = {
    var tx: Transaction = null
    try {
      tx = s.beginTransaction()
      f(tx)
      tx.commit()
    } catch {
      case NonFatal(e) =>
        Try(tx.rollback())
        throw new SparkException(s"Failed to execution tx because: ${e.getMessage}")
    } finally {
      if (tx != null) {
        tx.close()
      }
    }
  }

  protected def resetNeo4jDbState(): Unit = {
    withSession { s =>
      withTx(s) { tx =>
        val constraints = tx.run("SHOW ALL CONSTRAINT YIELD name").asScala
          .map { r => r.get("name").asString }
        constraints.foreach { c =>
          tx.run(s"DROP CONSTRAINT $c IF EXISTS")
        }
      }
      withTx(s) { tx =>
        tx.run("MATCH (n) DETACH DELETE n")
      }
    }
  }
}

case class Neo4jAuraSink(uri: String, user: String, passwd: String)
  extends BaseGraphBatchSink with BaseGraphStreamSink with  Neo4jAura with Logging {

  override def toString: String = {
    s"${this.getClass.getSimpleName}(uri=$uri, user=$user)"
  }

  private def genLabel(n: SQLFlowGraphNode): String = n.tpe match {
    case GraphNodeType.TableNode => "Table"
    case GraphNodeType.ViewNode => "View"
    case GraphNodeType.PlanNode => "Plan"
    case GraphNodeType.LeafPlanNode => "LeafPlan"
    case GraphNodeType.QueryNode => "Query"
  }

  private def genAttributeNames(n: SQLFlowGraphNode, w: String = ""): String = {
    n.attributeNames.map(a => s"""$w$a$w""").mkString(",")
  }

  private def genProps(n: SQLFlowGraphNode): String = {
    val basicProps = s"""name: "${n.ident}", uid: "${n.uniqueId}", """ +
      s"""attributeNames: [${genAttributeNames(n, "\"")}], schemaDDL: "${n.schemaDDL}""""
    if (n.props.nonEmpty) {
      val nodePropsAsJson = n.props.map { case (k, v) => s"""$k: "$v""""}.mkString(", ")
      s"$basicProps, $nodePropsAsJson"
    } else {
      basicProps
    }
  }

  private def tryToCreateConstraints(s: Session): Unit = try {
    withTx(s) { tx =>
      tx.run(
        s"""
           |CREATE CONSTRAINT unique_table_node_constraint IF NOT EXISTS
           |FOR (n:Table)
           |REQUIRE n.uid IS UNIQUE
         """.stripMargin)
      tx.run(
        s"""
           |CREATE CONSTRAINT unique_view_node_constraint IF NOT EXISTS
           |FOR (n:View)
           |REQUIRE n.uid IS UNIQUE
         """.stripMargin)
      tx.run(
        s"""
           |CREATE CONSTRAINT unique_plan_node_constraint IF NOT EXISTS
           |FOR (n:Plan)
           |REQUIRE n.semanticHash IS UNIQUE
         """.stripMargin)
      tx.run(
        s"""
           |CREATE CONSTRAINT unique_leaf_plan_node_constraint IF NOT EXISTS
           |FOR (n:LeafPlan)
           |REQUIRE n.semanticHash IS UNIQUE
         """.stripMargin)
    }
  } catch {
    case NonFatal(_) =>
  }

  private def tryToCreateNodes(s: Session, nodes: Seq[SQLFlowGraphNode]): Unit = {
    nodes.foreach { n =>
      try {
        withTx(s) { tx =>
          createNodes(tx, n :: Nil)
        }
      } catch {
        case NonFatal(_) =>
      }
    }
  }

  private def createNodes(tx: Transaction, nodes: Seq[SQLFlowGraphNode]): Unit = {
    nodes.foreach { n =>
      tx.run(s"""CREATE (:${genLabel(n)} { ${genProps(n)} })""")
    }
  }

  private def createEdges(
      tx: Transaction,
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge]): Unit = {
    val nodeMap = nodes.map { n =>
      n.uniqueId -> (genLabel(n), n.tpe match {
        case GraphNodeType.PlanNode | GraphNodeType.LeafPlanNode =>
          s"""semanticHash = "${n.props("semanticHash")}""""
        case _ =>
          s"""uid = "${n.uniqueId}""""
      })
    }.toMap

    val compactEdges = edges.map { e => (e.fromId, e.toId) }.distinct
    compactEdges.foreach { case (fromId, toId) =>
      val (fromLabel, fromPred) = nodeMap(fromId)
      val (toLabel, toPred) = nodeMap(toId)
      tx.run(
        s"""
           |MATCH (src:$fromLabel), (dst:$toLabel)
           |WHERE src.$fromPred AND dst.$toPred
           |MERGE (src)-[r:transformInto]->(dst)
           |ON CREATE SET r.refCnt = 1
           |ON MATCH SET r.refCnt = r.refCnt + 1
           |RETURN r.refCnt
         """.stripMargin)
    }
  }

  private def isDatabaseEmpty(tx: Transaction): Boolean = {
    !tx.run("MATCH (n) RETURN 1 LIMIT 1").hasNext
  }

  override def write(
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Unit = {
    val overwrite = options.getOrElse("overwrite", "false").toBoolean
    withSession { s =>
      if (!overwrite) {
        withTx(s) { tx =>
          if (!isDatabaseEmpty(tx)) {
            throw new AnalysisException("Database should be empty")
          }
        }
      } else {
        resetNeo4jDbState()
      }
      withTx(s) { tx =>
        // TODO: For fast queries, creates indexes on a generated graph
        createNodes(tx, nodes)
        createEdges(tx, nodes, edges)
      }
    }
  }

  override def append(
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Unit = {
    withSession { s =>
      tryToCreateConstraints(s)
      tryToCreateNodes(s, nodes)
      withTx(s) { tx =>
        createEdges(tx, nodes, edges)
      }
    }
  }
}
