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
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.flow._
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}

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
        df.saveAsSQLFlow(Map("outputDirPath" -> outputPath), graphSink = GraphVizSink(imageFormat))
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
          SQLFlow.saveAsSQLFlow(
            Map("outputDirPath" -> outputPath),
            graphSink = GraphVizSink(imageFormat)
          )
          val imgFile = new File(s"$outputPath/sqlflow.$imageFormat")
          assert(imgFile.exists())
        }
      }
    }
  }

  private def checkGraphVizFormatString(actual: String, expected: String): Unit = {
    val edgePattern = """".+":.+ -> ".+":.+"""
    super.checkOutputString(edgePattern)(actual, expected)
  }

  test("graphviz format - stream") {
    withTempDir { tmpDir =>
      val rootDirPath = tmpDir.getAbsolutePath
      withListener(SQLFlowListener(GraphVizSink(), options = Map("outputDirPath" -> rootDirPath))) {
        val df1 = spark.range(1).selectExpr("id as k", "id as v")
        checkAnswer(df1, Row(0, 0) :: Nil)
        spark.sparkContext.listenerBus.waitUntilEmpty()
        assert(new File(rootDirPath).list().length === 1)

        val df2 = spark.range(1)
          .selectExpr("id as k", "id as v")
          .groupBy("k")
          .count()
        checkAnswer(df2, Row(0, 1) :: Nil)
        spark.sparkContext.listenerBus.waitUntilEmpty()
        assert(new File(rootDirPath).list().length === 2)

        val Seq(outputDirPath1, outputDirPath2) = new File(rootDirPath).list().sorted.toSeq
        val outputPath1 = new File(new File(rootDirPath, outputDirPath1), "sqlflow.dot")
        assert(outputPath1.exists())
        val flowString1 = fileToString(outputPath1)
        checkGraphVizFormatString(flowString1,
          s"""
             |digraph {
             |  graph [pad="0.5" nodesep="0.5" ranksep="1" fontname="Helvetica" rankdir=LR];
             |  node [shape=plaintext]
             |
             |"Project_e2b137e" [label=<
             |<table color="lightgray" border="1" cellborder="0" cellspacing="0">
             |  <tr><td bgcolor="lightgray" port="nodeName"><i>Project</i></td></tr>
             |  <tr><td port="0">k</td></tr>
             |<tr><td port="1">v</td></tr>
             |</table>>];
             |
             |"Range_7018cf1" [label=<
             |<table color="lightgray" border="1" cellborder="0" cellspacing="0">
             |  <tr><td bgcolor="lightgray" port="nodeName"><i>Range</i></td></tr>
             |  <tr><td port="0">id</td></tr>
             |</table>>];
             |
             |"query_7b0731c" [color="black" label=<
             |<table>
             |  <tr><td bgcolor="black" port="nodeName"><i><font color="white">query_404581623</font></i></td></tr>
             |  <tr><td port="0">k</td></tr>
             |<tr><td port="1">v</td></tr>
             |</table>>];
             |
             |"Project_e2b137e":0 -> "query_7b0731c":0;
             |"Project_e2b137e":1 -> "query_7b0731c":1;
             |"Range_7018cf1":0 -> "Project_e2b137e":0;
             |"Range_7018cf1":0 -> "Project_e2b137e":1;
             |}
           """.stripMargin)

        val outputPath2 = new File(new File(rootDirPath, outputDirPath2), "sqlflow.dot")
        assert(outputPath2.exists())
        val flowString2 = fileToString(outputPath2)
        checkGraphVizFormatString(flowString2,
          s"""
             |digraph {
             |  graph [pad="0.5" nodesep="0.5" ranksep="1" fontname="Helvetica" rankdir=LR];
             |  node [shape=plaintext]
             |
             |"Aggregate_09b069b" [label=<
             |<table color="lightgray" border="1" cellborder="0" cellspacing="0">
             |  <tr><td bgcolor="lightgray" port="nodeName"><i>Aggregate</i></td></tr>
             |  <tr><td port="0">k</td></tr>
             |<tr><td port="1">count</td></tr>
             |</table>>];
             |
             |"Project_8f8c331" [label=<
             |<table color="lightgray" border="1" cellborder="0" cellspacing="0">
             |  <tr><td bgcolor="lightgray" port="nodeName"><i>Project</i></td></tr>
             |  <tr><td port="0">k</td></tr>
             |</table>>];
             |
             |"Range_b1033f6" [label=<
             |<table color="lightgray" border="1" cellborder="0" cellspacing="0">
             |  <tr><td bgcolor="lightgray" port="nodeName"><i>Range</i></td></tr>
             |  <tr><td port="0">id</td></tr>
             |</table>>];
             |
             |"query_a88630b" [color="black" label=<
             |<table>
             |  <tr><td bgcolor="black" port="nodeName"><i><font color="white">query_1122462682</font></i></td></tr>
             |  <tr><td port="0">k</td></tr>
             |<tr><td port="1">count</td></tr>
             |</table>>];
             |
             |"Aggregate_09b069b":0 -> "query_a88630b":0;
             |"Aggregate_09b069b":1 -> "query_a88630b":1;
             |"Project_8f8c331":0 -> "Aggregate_09b069b":0;
             |"Range_b1033f6":0 -> "Project_8f8c331":0;
             |}
           """.stripMargin)
      }
    }
  }

  private def checkAdjListFormatString(actual: String, expected: String): Unit = {
    val edgePattern = """[a-zA-Z0-9_]+:[a-zA-Z0-9_]+"""
    super.checkOutputString(edgePattern)(actual, expected)
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
          SQLFlow.printAsSQLFlow(graphFormat = AdjacencyListSink(sep = ':'))
        }
        checkAdjListFormatString(flowString,
          """
            |t_8ab1007:Aggregate_b428a70
            |Aggregate_b428a70:default.t1
            |Filter_8f6c734:Project_f8bdc51
            |Project_f8bdc51:Aggregate_227c6c5
            |Aggregate_227c6c5:t3
            |default.t1:Project_41d45bc
            |Project_41d45bc:t2
            |t2:Filter_8f6c734
          """.stripMargin)

       val contractedFlowString = getOutputAsString {
          SQLFlow.printAsSQLFlow(
            contracted = true, graphFormat = AdjacencyListSink(sep = ":"))
        }
        checkAdjListFormatString(contractedFlowString,
          """
            |t_0:default.t1
            |t2:t3
            |default.t1:t2
          """.stripMargin)
      }
    }
  }
}
