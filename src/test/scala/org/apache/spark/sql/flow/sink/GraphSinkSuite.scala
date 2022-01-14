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

import java.io.File

import org.apache.spark.TestUtils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.flow._
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class GraphSinkSuite extends QueryTest with SharedSparkSession
  with SQLTestUtils with SQLFlowTestUtils {

  test("graphviz image data generation") {
    assume(TestUtils.testCommandAvailable("dot"))

    val testImageFormats = Seq("svg", "png", "jpg")

    withTempDir { dirPath =>
      import org.apache.spark.sql.flow.SQLFlow._
      val df = sql("SELECT k, sum(v) FROM VALUES (1, 2), (3, 4) t(k, v) GROUP BY k")

      testImageFormats.foreach { imageFormat =>
        val outputPath = s"${dirPath.getAbsolutePath}/$imageFormat"
        df.saveAsSQLFlow(outputPath, graphSink = GraphVizSink(imageFormat))
        val imgFile = new File(s"$outputPath/sqlflow.$imageFormat")
        assert(imgFile.exists())
      }
    }
    withTempView("t") {
      withTempDir { dirPath =>
        sql(
          """
            |CREATE OR REPLACE TEMPORARY VIEW t AS
            |  SELECT k, sum(v) FROM VALUES (1, 2), (3, 4) t(k, v) GROUP BY k
          """.stripMargin)

        testImageFormats.foreach { imageFormat =>
          val outputPath = s"${dirPath.getAbsolutePath}/$imageFormat"
          SQLFlow.saveAsSQLFlow(outputPath, graphSink = GraphVizSink(imageFormat))
          val imgFile = new File(s"$outputPath/sqlflow.$imageFormat")
          assert(imgFile.exists())
        }
      }
    }
  }

  test("adjacency list format") {
    withView("t1") {
      withTempView("t2", "t3") {
        sql("CREATE VIEW t1 AS SELECT k, SUM(v) sum FROM VALUES (1, 2), (2, 3) t(k, v) GROUP BY k")
        sql("CREATE TEMPORARY VIEW t2 AS SELECT k, sum, rand() v2 FROM t1")

        val df = spark.table("t2")
          .where("k > 1")
          .cache()

        df.groupBy("sum")
          .count()
          .createOrReplaceTempView("t3")

        val flowString = getOutputAsString {
          SQLFlow.printAsSQLFlow(graphFormat = AdjacencyListFormat(sep = ':'))
        }
        checkOutputString(flowString,
          """
            |Project_3:Aggregate_4
            |t2:Filter_2
            |Filter_2:Project_3
            |Aggregate_4:t3
            |Aggregate_1:default.t1
            |default.t1:Project_5
            |t_0:Aggregate_1
            |Project_5:t2
          """.stripMargin)

       val contractedFlowString = getOutputAsString {
          SQLFlow.printAsSQLFlow(
            contracted = true, graphFormat = AdjacencyListFormat(sep = ":"))
        }
        checkOutputString(contractedFlowString,
          """
            |t_0:default.t1
            |t2:t3
            |default.t1:t2
          """.stripMargin)
      }
    }
  }
}
