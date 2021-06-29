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

package org.apache.spark.sql

import java.io.File

import org.apache.spark.TestUtils
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class SQLFlowSuite extends QueryTest with SharedSparkSession with SQLTestUtils {

  private def getOutputAsString(f: => Unit): String = {
    val output = new java.io.ByteArrayOutputStream()
    Console.withOut(output) { f }
    output.toString
  }

  private def checkOutputString(actual: String, expected: String): Unit = {
    def normalize(s: String) = s.replaceAll("_\\d+", "_x").replaceAll(" ", "").replaceAll("\n", "")
    assert(normalize(actual) == normalize(expected),
      s"`$actual` didn't match an expected string `$expected`")
  }

  test("df.printAsSQLFlow") {
    import SQLFlow._
    val flowString = getOutputAsString {
      val df = sql("SELECT k, sum(v) FROM VALUES (1, 2), (3, 4) t(k, v) GROUP BY k")
      df.debugPrintAsSQLFlow()
    }
    checkOutputString(flowString,
      s"""
         |digraph {
         |  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
         |  node [shape=plain]
         |  rankdir=LR;
         |
         |  "Aggregate_x" [label=<
         |  <table border="1" cellborder="0" cellspacing="0">
         |    <tr><td bgcolor="lightgray" port="nodeName"><i>Aggregate_x</i></td></tr>
         |    <tr><td port="0">k</td></tr>
         |  <tr><td port="1">sum(v)</td></tr>
         |  </table>>];
         |
         |
         |  "LocalRelation_x" [label=<
         |  <table border="1" cellborder="0" cellspacing="0">
         |    <tr><td bgcolor="lightpink" port="nodeName"><i>LocalRelation_x</i></td></tr>
         |    <tr><td port="0">k</td></tr>
         |  <tr><td port="1">v</td></tr>
         |  </table>>];
         |
         |  "LocalRelation_x":0 -> "Aggregate_x":0;
         |  "LocalRelation_x":1 -> "Aggregate_x":1;
         |}
       """.stripMargin)
  }

  test("SQLFlow.printAsSQLFlow") {
    withTempView("t") {
      sql(s"""
           |CREATE OR REPLACE TEMPORARY VIEW t AS
           |  SELECT k, sum(v) FROM VALUES (1, 2), (3, 4) t(k, v) GROUP BY k
         """.stripMargin)

      val flowString = getOutputAsString {
        SQLFlow.debugPrintAsSQLFlow()
      }
      checkOutputString(flowString,
        s"""digraph {
           |  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
           |  node [shape=plain]
           |  rankdir=LR;
           |
           |  "Aggregate_x" [label=<
           |  <table border="1" cellborder="0" cellspacing="0">
           |    <tr><td bgcolor="lightgray" port="nodeName"><i>Aggregate_x</i></td></tr>
           |    <tr><td port="0">k</td></tr>
           |  <tr><td port="1">sum(v)</td></tr>
           |  </table>>];
           |
           |
           |  "t" [label=<
           |  <table border="1" cellborder="0" cellspacing="0">
           |    <tr><td bgcolor="lightyellow" port="nodeName"><i>t</i></td></tr>
           |    <tr><td port="0">k</td></tr>
           |  <tr><td port="1">sum(v)</td></tr>
           |  </table>>];
           |
           |
           |  "t" [label=<
           |  <table border="1" cellborder="0" cellspacing="0">
           |    <tr><td bgcolor="lightyellow" port="nodeName"><i>t</i></td></tr>
           |    <tr><td port="0">k</td></tr>
           |  <tr><td port="1">v</td></tr>
           |  </table>>];
           |
           |  "Aggregate_x":0 -> "t":0;
           |  "Aggregate_x":1 -> "t":1;
           |  "t":0 -> "Aggregate_x":0;
           |  "t":1 -> "Aggregate_x":1;
           |}
         """.stripMargin)
    }
  }

  test("df.saveAsSQLFlow") {
    withTempDir { dirPath =>
      import SQLFlow._
      val df = sql("SELECT k, sum(v) FROM VALUES (1, 2), (3, 4) t(k, v) GROUP BY k")

      df.saveAsSQLFlow(s"${dirPath.getAbsolutePath}/d")
      val flowString = fileToString(new File(s"${dirPath.getAbsolutePath}/d/sqlflow.dot"))
      checkOutputString(flowString,
        s"""
           |digraph {
           |  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
           |  node [shape=plain]
           |  rankdir=LR;
           |
           |  "Aggregate_x" [label=<
           |  <table border="1" cellborder="0" cellspacing="0">
           |    <tr><td bgcolor="lightgray" port="nodeName"><i>Aggregate_x</i></td></tr>
           |    <tr><td port="0">k</td></tr>
           |  <tr><td port="1">sum(v)</td></tr>
           |  </table>>];
           |
           |
           |  "LocalRelation_x" [label=<
           |  <table border="1" cellborder="0" cellspacing="0">
           |    <tr><td bgcolor="lightpink" port="nodeName"><i>LocalRelation_x</i></td></tr>
           |    <tr><td port="0">k</td></tr>
           |  <tr><td port="1">v</td></tr>
           |  </table>>];
           |
           |  "LocalRelation_x":0 -> "Aggregate_x":0;
           |  "LocalRelation_x":1 -> "Aggregate_x":1;
           |}
         """.stripMargin)
    }
  }

  test("SQLFlow.saveAsSQLFlow") {
    withTempView("t") {
      withTempDir { dirPath =>
        sql(s"""
             |CREATE OR REPLACE TEMPORARY VIEW t AS
             |  SELECT k, sum(v) FROM VALUES (1, 2), (3, 4) t(k, v) GROUP BY k
           """.stripMargin)

        SQLFlow.saveAsSQLFlow(s"${dirPath.getAbsolutePath}/d")
        val flowString = fileToString(new File(s"${dirPath.getAbsolutePath}/d/sqlflow.dot"))
        checkOutputString(flowString,
          s"""digraph {
             |  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
             |  node [shape=plain]
             |  rankdir=LR;
             |
             |  "Aggregate_x" [label=<
             |  <table border="1" cellborder="0" cellspacing="0">
             |    <tr><td bgcolor="lightgray" port="nodeName"><i>Aggregate_x</i></td></tr>
             |    <tr><td port="0">k</td></tr>
             |  <tr><td port="1">sum(v)</td></tr>
             |  </table>>];
             |
             |  "t" [label=<
             |  <table border="1" cellborder="0" cellspacing="0">
             |    <tr><td bgcolor="lightyellow" port="nodeName"><i>t</i></td></tr>
             |    <tr><td port="0">k</td></tr>
             |  <tr><td port="1">sum(v)</td></tr>
             |  </table>>];
             |
             |  "t" [label=<
             |  <table border="1" cellborder="0" cellspacing="0">
             |    <tr><td bgcolor="lightyellow" port="nodeName"><i>t</i></td></tr>
             |    <tr><td port="0">k</td></tr>
             |  <tr><td port="1">v</td></tr>
             |  </table>>];
             |
             |  "Aggregate_x":0 -> "t":0;
             |  "Aggregate_x":1 -> "t":1;
             |  "t":0 -> "Aggregate_x":0;
             |  "t":1 -> "Aggregate_x":1;
             |}
           """.stripMargin)
      }
    }
  }

  test("path already exists") {
    withTempDir { dir =>
      import SQLFlow._
      val errMsg1 = intercept[AnalysisException] {
        spark.range(1).saveAsSQLFlow(dir.getAbsolutePath)
      }.getMessage
      assert(errMsg1.contains(" already exists"))
      val errMsg2 = intercept[AnalysisException] {
        SQLFlow.saveAsSQLFlow(dir.getAbsolutePath)
      }.getMessage
      assert(errMsg2.contains(" already exists"))
    }
  }

  test("invalid image format") {
    withTempDir { dir =>
      import SQLFlow._
      val errMsg1 = intercept[AnalysisException] {
        spark.range(1).saveAsSQLFlow(s"${dir.getAbsolutePath}/d", format = "invalid")
      }.getMessage
      assert(errMsg1.contains("Invalid image format: invalid"))
      val errMsg2 = intercept[AnalysisException] {
        SQLFlow.saveAsSQLFlow(s"${dir.getAbsolutePath}/d", format = "invalid")
      }.getMessage
      assert(errMsg2.contains("Invalid image format: invalid"))
    }
  }

  test("image data generation") {
    assume(TestUtils.testCommandAvailable("dot"))
    withTempDir { dirPath =>
      import SQLFlow._
      val df = sql("SELECT k, sum(v) FROM VALUES (1, 2), (3, 4) t(k, v) GROUP BY k")

      SQLFlow.validImageFormatSet.foreach { format =>
        val outputPath = s"${dirPath.getAbsolutePath}/$format"
        df.saveAsSQLFlow(outputPath, format)
        val imgFile = new File(s"${outputPath}/sqlflow.$format")
        assert(imgFile.exists())
      }
    }
    withTempView("t") {
      withTempDir { dirPath =>
        sql(
          s"""
             |CREATE OR REPLACE TEMPORARY VIEW t AS
             |  SELECT k, sum(v) FROM VALUES (1, 2), (3, 4) t(k, v) GROUP BY k
           """.stripMargin)

        SQLFlow.validImageFormatSet.foreach { format =>
          val outputPath = s"${dirPath.getAbsolutePath}/$format"
          SQLFlow.saveAsSQLFlow(outputPath, format)
          val imgFile = new File(s"${outputPath}/sqlflow.$format")
          assert(imgFile.exists())
        }
      }
    }
  }
}
