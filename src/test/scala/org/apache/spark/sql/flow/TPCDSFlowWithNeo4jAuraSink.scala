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

import java.io.File

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.flow.sink.Neo4jAuraSink
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class TPCDSFlowWithNeo4jAuraSink extends QueryTest with SharedSparkSession
  with SQLTestUtils with SQLFlowTestUtils with Neo4jAuraTest with TPCDSTest {

  private lazy val tpcdsQueryFiles = {
    val tpcdsResourceFile = getWorkspaceFilePath(
      tpcdsResourceFilePath.head, tpcdsResourceFilePath.tail: _*).toFile
    val queryFiles = new File(tpcdsResourceFile, "inputs")
    queryFiles.listFiles().filter(_.getName.endsWith(".sql"))
  }

  private def splitWithSemicolon(seq: Seq[String]) = {
    seq.mkString("\n").split("(?<=[^\\\\]);")
  }

  private def splitCommentsAndCodes(input: String) = {
    input.split("\n").partition(_.trim.startsWith("--"))
  }

  private def getQueryFromFile(f: File): String = {
    val input = fileToString(f)
    val (_, code) = splitCommentsAndCodes(input)
    val queries = splitWithSemicolon(code).toSeq.map(_.trim).filter(_ != "").toSeq
      // Fix misplacement when comment is at the end of the query.
      .map(_.split("\n").filterNot(_.startsWith("--")).mkString("\n")).map(_.trim).filter(_ != "")
    if (queries.length > 1) {
      logWarning(s"""Some queries ignored: ${queries.tail.mkString("\n")}""")
    }
    queries.head
  }

  test("should generate data lineage of TPCDS queries") {
    tpcdsQueryFiles.foreach { file =>
      val query = getQueryFromFile(file)
      spark.sql(query).createOrReplaceTempView(file.getName.replace(".sql", ""))
    }
    val sink = Neo4jAuraSink(uri, user, passwd)
    SQLFlow.saveAsSQLFlow(graphSink = sink)

    withSession { s =>
      withTx(s) { tx =>
        val r1 = tx.run("MATCH(n:Table) RETURN count(n) AS cnt")
        assert(r1.single().get("cnt").asInt === 24)

        val r2 = tx.run("MATCH(n:View) RETURN count(n) AS cnt")
        assert(r2.single().get("cnt").asInt === 103)

        // TODO: The checks below are pretty slow, so we need to create indexes
        // on the generated graph for fast queries.
        //
        // tpcdsQueryFiles.map(_.getName.replace(".sql", "")).foreach { q =>
        //   val r = tx.run(
        //     s"""
        //        |MATCH p=(:Table)-[*]->(:View {name: "$q"})
        //        |RETURN count(p) AS cnt
        //      """.stripMargin)
        //   assert(r.single().get("cnt").asInt > 0)
        // }
      }
    }
  }
}
