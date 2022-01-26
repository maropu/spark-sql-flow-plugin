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

  private def getWorkspaceFilePath(first: String, more: String*) = {
    if (!(sys.props.contains("spark.test.home") || sys.env.contains("SPARK_HOME"))) {
      fail("spark.test.home or SPARK_HOME is not set.")
    }
    val sparkHome = sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
    java.nio.file.Paths.get(sparkHome, first +: more: _*)
  }

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

  ignore("should generate data lineage of TPCDS queries") {
    tpcdsQueryFiles.foreach { file =>
      val query = getQueryFromFile(file)
      spark.sql(query).createOrReplaceTempView(file.getName.replace(".sql", ""))
    }

    // scalastyle:off line.size.limit
    val qeuryToRelationMap = Map(
      "q30" -> Seq("default.web_returns", "default.date_dim", "default.customer_address", "default.customer"),
      "q18" -> Seq("default.catalog_sales", "default.customer_demographics", "default.customer", "default.customer_address", "default.date_dim", "default.item"),
      "q7" -> Seq("default.store_sales", "default.customer_demographics", "default.date_dim", "default.item", "default.promotion"),
      "q6" -> Seq("default.customer_address", "default.customer", "default.store_sales", "default.date_dim", "default.item"),
      "q19" -> Seq("default.date_dim", "default.store_sales", "default.item", "default.customer", "default.customer_address", "default.store"),
      "q25" -> Seq("default.store_sales", "default.store_returns", "default.catalog_sales", "default.date_dim", "default.store", "default.item"),
      "q31" -> Seq("default.store_sales", "default.date_dim", "default.customer_address", "default.web_sales"),
      "q27" -> Seq("default.store_sales", "default.customer_demographics", "default.date_dim", "default.store", "default.item"),
      "q33" -> Seq("default.store_sales", "default.date_dim", "default.customer_address", "default.item", "default.catalog_sales", "default.web_sales"),
      "q4" -> Seq("default.customer", "default.store_sales", "default.date_dim", "default.catalog_sales", "default.web_sales"),
      "q5" -> Seq("default.store_sales", "default.store_returns", "default.date_dim", "default.store", "default.catalog_sales", "default.catalog_returns", "default.catalog_page", "default.web_sales", "default.web_returns", "default.web_site"),
      "q32" -> Seq("default.catalog_sales", "default.item", "default.date_dim"),
      "q26" -> Seq("default.catalog_sales", "default.customer_demographics", "default.date_dim", "default.item", "default.promotion"),
      "q22" -> Seq("default.inventory", "default.date_dim", "default.item", "default.warehouse"),
      "q36" -> Seq("default.store_sales", "default.date_dim", "default.item", "default.store"),
      "q1" -> Seq("default.store_returns", "default.date_dim", "default.store", "default.customer"),
      "q37" -> Seq("default.item", "default.inventory", "default.date_dim", "default.catalog_sales"),
      "q35" -> Seq("default.customer", "default.store_sales", "default.date_dim", "default.web_sales", "default.catalog_sales", "default.customer_address", "default.customer_demographics"),
      "q21" -> Seq("default.inventory", "default.warehouse", "default.item", "default.date_dim"),
      "q2" -> Seq("default.web_sales", "default.catalog_sales", "default.date_dim"),
      "q3" -> Seq("default.date_dim", "default.store_sales", "default.item"),
      "q20" -> Seq("default.catalog_sales", "default.item", "default.date_dim"),
      "q34" -> Seq("default.store_sales", "default.date_dim", "default.store", "default.household_demographics", "default.customer"),
      "q90" -> Seq("default.web_sales", "default.household_demographics", "default.time_dim", "default.web_page"),
      "q84" -> Seq("default.customer", "default.customer_address", "default.customer_demographics", "default.household_demographics", "default.income_band", "default.store_returns"),
      "q53" -> Seq("default.item", "default.store_sales", "default.date_dim", "default.store"),
      "q47" -> Seq("default.item", "default.store_sales", "default.date_dim", "default.store"),
      "q46" -> Seq("default.store_sales", "default.date_dim", "default.store", "default.household_demographics", "default.customer_address", "default.customer"),
      "q52" -> Seq("default.date_dim", "default.store_sales", "default.item"),
      "q85" -> Seq("default.web_sales", "default.web_returns", "default.web_page", "default.customer_demographics", "default.customer_address", "default.date_dim", "default.reason"),
      "q91" -> Seq("default.call_center", "default.catalog_returns", "default.date_dim", "default.customer", "default.customer_address", "default.customer_demographics", "default.household_demographics"),
      "q87" -> Seq("default.store_sales", "default.date_dim", "default.customer", "default.catalog_sales", "default.web_sales"),
      "q93" -> Seq("default.store_sales", "default.store_returns", "default.reason"),
      "q44" -> Seq("default.store_sales", "default.item"),
      "q50" -> Seq("default.store_sales", "default.store_returns", "default.store", "default.date_dim"),
      "q78" -> Seq("default.store_sales", "default.store_returns", "default.date_dim", "default.web_sales", "default.web_returns", "default.catalog_sales", "default.catalog_returns"),
      "q79" -> Seq("default.store_sales", "default.date_dim", "default.store", "default.household_demographics", "default.customer"),
      "q51" -> Seq("default.web_sales", "default.date_dim", "default.store_sales"),
      "q45" -> Seq("default.web_sales", "default.customer", "default.customer_address", "default.date_dim", "default.item"),
      "q92" -> Seq("default.web_sales", "default.item", "default.date_dim"),
      "q86" -> Seq("default.web_sales", "default.date_dim", "default.item"),
      "q82" -> Seq("default.item", "default.inventory", "default.date_dim", "default.store_sales"),
      "q96" -> Seq("default.store_sales", "default.household_demographics", "default.time_dim", "default.store"),
      "q69" -> Seq("default.customer", "default.store_sales", "default.date_dim", "default.web_sales", "default.catalog_sales", "default.customer_address", "default.customer_demographics"),
      "q41" -> Seq("default.item"),
      "q55" -> Seq("default.date_dim", "default.store_sales", "default.item"),
      "q54" -> Seq("default.catalog_sales", "default.web_sales", "default.item", "default.date_dim", "default.customer", "default.store_sales", "default.customer_address", "default.store"),
      "q40" -> Seq("default.catalog_sales", "default.catalog_returns", "default.warehouse", "default.item", "default.date_dim"),
      "q68" -> Seq("default.store_sales", "default.date_dim", "default.store", "default.household_demographics", "default.customer_address", "default.customer"),
      "q97" -> Seq("default.store_sales", "default.date_dim", "default.catalog_sales"),
      "q83" -> Seq("default.store_returns", "default.item", "default.date_dim", "default.catalog_returns", "default.web_returns"),
      "q95" -> Seq("default.web_sales", "default.web_returns", "default.date_dim", "default.customer_address", "default.web_site"),
      "q81" -> Seq("default.catalog_returns", "default.date_dim", "default.customer_address", "default.customer"),
      "q56" -> Seq("default.store_sales", "default.date_dim", "default.customer_address", "default.item", "default.catalog_sales", "default.web_sales"),
      "q42" -> Seq("default.date_dim", "default.store_sales", "default.item"),
      "q43" -> Seq("default.date_dim", "default.store_sales", "default.store"),
      "q57" -> Seq("default.item", "default.catalog_sales", "default.date_dim", "default.call_center"),
      "q80" -> Seq("default.store_sales", "default.store_returns", "default.date_dim", "default.store", "default.item", "default.promotion", "default.catalog_sales", "default.catalog_returns", "default.catalog_page", "default.web_sales", "default.web_returns", "default.web_site"),
      "q94" -> Seq("default.web_sales", "default.web_returns", "default.date_dim", "default.customer_address", "default.web_site"),
      "q99" -> Seq("default.catalog_sales", "default.warehouse", "default.ship_mode", "default.call_center", "default.date_dim"),
      "q72" -> Seq("default.catalog_sales", "default.inventory", "default.warehouse", "default.item", "default.customer_demographics", "default.household_demographics", "default.date_dim", "default.promotion", "default.catalog_returns"),
      "q66" -> Seq("default.web_sales", "default.warehouse", "default.date_dim", "default.time_dim", "default.ship_mode", "default.catalog_sales"),
      "q24b" -> Seq("default.store_sales", "default.store_returns", "default.store", "default.item", "default.customer", "default.customer_address"),
      "q67" -> Seq("default.store_sales", "default.date_dim", "default.store", "default.item"),
      "q73" -> Seq("default.store_sales", "default.date_dim", "default.store", "default.household_demographics", "default.customer"),
      "q98" -> Seq("default.store_sales", "default.item", "default.date_dim"),
      "q65" -> Seq("default.store", "default.store_sales", "default.date_dim", "default.item"),
      "q71" -> Seq("default.item", "default.web_sales", "default.date_dim", "default.catalog_sales", "default.store_sales", "default.time_dim"),
      "q59" -> Seq("default.store_sales", "default.date_dim", "default.store"),
      "q24a" -> Seq("default.store_sales", "default.store_returns", "default.store", "default.item", "default.customer", "default.customer_address"),
      "q58" -> Seq("default.store_sales", "default.item", "default.date_dim", "default.catalog_sales", "default.web_sales"),
      "q70" -> Seq("default.store_sales", "default.date_dim", "default.store"),
      "q64" -> Seq("default.store_sales", "default.store_returns", "default.catalog_sales", "default.catalog_returns", "default.date_dim", "default.store", "default.customer", "default.customer_demographics", "default.promotion", "default.household_demographics", "default.customer_address", "default.income_band", "default.item"),
      "q48" -> Seq("default.store_sales", "default.store", "default.customer_demographics", "default.customer_address", "default.date_dim"),
      "q60" -> Seq("default.store_sales", "default.date_dim", "default.customer_address", "default.item", "default.catalog_sales", "default.web_sales"),
      "q74" -> Seq("default.customer", "default.store_sales", "default.date_dim", "default.web_sales"),
      "q75" -> Seq("default.catalog_sales", "default.item", "default.date_dim", "default.catalog_returns", "default.store_sales", "default.store_returns", "default.web_sales", "default.web_returns"),
      "q61" -> Seq("default.store_sales", "default.store", "default.promotion", "default.date_dim", "default.customer", "default.customer_address", "default.item"),
      "q49" -> Seq("default.web_sales", "default.web_returns", "default.date_dim", "default.catalog_sales", "default.catalog_returns", "default.store_sales", "default.store_returns"),
      "q88" -> Seq("default.store_sales", "default.household_demographics", "default.time_dim", "default.store"),
      "q77" -> Seq("default.store_sales", "default.date_dim", "default.store", "default.store_returns", "default.catalog_sales", "default.catalog_returns", "default.web_sales", "default.web_page", "default.web_returns"),
      "q63" -> Seq("default.item", "default.store_sales", "default.date_dim", "default.store"),
      "q62" -> Seq("default.web_sales", "default.warehouse", "default.ship_mode", "default.web_site", "default.date_dim"),
      "q76" -> Seq("default.store_sales", "default.item", "default.date_dim", "default.web_sales", "default.catalog_sales"),
      "q89" -> Seq("default.item", "default.store_sales", "default.date_dim", "default.store"),
      "q11" -> Seq("default.customer", "default.store_sales", "default.date_dim", "default.web_sales"),
      "q14b" -> Seq("default.store_sales", "default.item", "default.catalog_sales", "default.date_dim", "default.web_sales"),
      "q38" -> Seq("default.store_sales", "default.date_dim", "default.customer", "default.catalog_sales", "default.web_sales"),
      "q10" -> Seq("default.customer", "default.store_sales", "default.date_dim", "default.web_sales", "default.catalog_sales", "default.customer_address", "default.customer_demographics"),
      "q12" -> Seq("default.web_sales", "default.item", "default.date_dim"),
      "q14a" -> Seq("default.store_sales", "default.item", "default.catalog_sales", "default.date_dim", "default.web_sales"),
      "q13" -> Seq("default.store_sales", "default.store", "default.customer_address", "default.date_dim", "default.customer_demographics", "default.household_demographics"),
      "q17" -> Seq("default.store_sales", "default.store_returns", "default.catalog_sales", "default.date_dim", "default.store", "default.item"),
      "q8" -> Seq("default.store_sales", "default.date_dim", "default.store", "default.customer_address", "default.customer"),
      "q23b" -> Seq("default.catalog_sales", "default.store_sales", "default.date_dim", "default.item", "default.customer", "default.web_sales"),
      "q39a" -> Seq("default.inventory", "default.item", "default.warehouse", "default.date_dim"),
      "q9" -> Seq("default.reason"),
      "q16" -> Seq("default.catalog_sales", "default.catalog_returns", "default.date_dim", "default.customer_address", "default.call_center"),
      "q28" -> Seq("default.store_sales"),
      "q23a" -> Seq("default.catalog_sales", "default.store_sales", "default.date_dim", "default.item", "default.customer", "default.web_sales"),
      "q39b" -> Seq("default.inventory", "default.item", "default.warehouse", "default.date_dim"),
      "q15" -> Seq("default.catalog_sales", "default.customer", "default.customer_address", "default.date_dim"),
      "q29" -> Seq("default.store_sales", "default.store_returns", "default.catalog_sales", "default.date_dim", "default.store", "default.item"),
    )
    // scalastyle:on line.size.limit

    val sink = Neo4jAuraSink(uri, user, passwd)
    SQLFlow.saveAsSQLFlow(graphSink = sink)

    withSession { s =>
      withTx(s) { tx =>
        val r1 = tx.run("MATCH(n:Table) RETURN count(n) AS cnt")
        assert(r1.single().get("cnt").asInt === 24)
        val r2 = tx.run("MATCH(n:View) RETURN count(n) AS cnt")
        assert(r2.single().get("cnt").asInt === 103)
      }

      // TODO: Skips the two tests because it is too slow
      val ignoredQuerySet = Set("q23a", "q23b")
      tpcdsQueryFiles.map(_.getName.replace(".sql", ""))
          .filterNot(ignoredQuerySet.contains).foreach { q =>
        withTx(s) { tx =>
          // TODO: Runs only a single test for each query because this test is too slow
          qeuryToRelationMap(q).take(1).foreach { rel =>
            val r = tx.run(
              s"""
                 |MATCH p=(:Table {name: "$rel"})-[:transformInto*]->(:View {name: "$q"})
                 |WHERE ALL(r IN relationships(p) WHERE "$q" IN r.dstNodeIds)
                 |RETURN p LIMIT 1
               """.stripMargin)
            assert(r.list().size === 1,
              s"Couldn't find a path from '$rel' to '$q'")
          }
        }
      }
    }
  }
}
