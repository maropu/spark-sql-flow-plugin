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
      df.printAsSQLFlow()
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
         |    <tr><td bgcolor="lightgray"><i>Aggregate_x</i></td></tr>
         |    <tr><td port="0">k</td></tr>
         |  <tr><td port="1">sum(v)</td></tr>
         |  </table>>];
         |
         |
         |  "LocalRelation_x" [label=<
         |  <table border="1" cellborder="0" cellspacing="0">
         |    <tr><td bgcolor="lightpink"><i>LocalRelation_x</i></td></tr>
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
        SQLFlow.printAsSQLFlow()
      }
      checkOutputString(flowString,
        s"""digraph {
           |  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
           |  node [shape=plain]
           |  rankdir=LR;
           |
           |  "Aggregate_x" [label=<
           |  <table border="1" cellborder="0" cellspacing="0">
           |    <tr><td bgcolor="lightgray"><i>Aggregate_x</i></td></tr>
           |    <tr><td port="0">k</td></tr>
           |  <tr><td port="1">sum(v)</td></tr>
           |  </table>>];
           |
           |
           |  "t" [label=<
           |  <table border="1" cellborder="0" cellspacing="0">
           |    <tr><td bgcolor="lightyellow"><i>t</i></td></tr>
           |    <tr><td port="0">k</td></tr>
           |  <tr><td port="1">sum(v)</td></tr>
           |  </table>>];
           |
           |
           |  "t" [label=<
           |  <table border="1" cellborder="0" cellspacing="0">
           |    <tr><td bgcolor="lightyellow"><i>t</i></td></tr>
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
    withTempPath { path =>
      import SQLFlow._
      val df = sql("SELECT k, sum(v) FROM VALUES (1, 2), (3, 4) t(k, v) GROUP BY k")
      df.saveAsSQLFlow(path.getAbsolutePath)

      val flowString = fileToString(path)
      checkOutputString(flowString,
        s"""
           |digraph {
           |  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
           |  node [shape=plain]
           |  rankdir=LR;
           |
           |  "Aggregate_x" [label=<
           |  <table border="1" cellborder="0" cellspacing="0">
           |    <tr><td bgcolor="lightgray"><i>Aggregate_x</i></td></tr>
           |    <tr><td port="0">k</td></tr>
           |  <tr><td port="1">sum(v)</td></tr>
           |  </table>>];
           |
           |
           |  "LocalRelation_x" [label=<
           |  <table border="1" cellborder="0" cellspacing="0">
           |    <tr><td bgcolor="lightpink"><i>LocalRelation_x</i></td></tr>
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
      withTempPath { path =>
        sql(s"""
             |CREATE OR REPLACE TEMPORARY VIEW t AS
             |  SELECT k, sum(v) FROM VALUES (1, 2), (3, 4) t(k, v) GROUP BY k
           """.stripMargin)

        SQLFlow.saveAsSQLFlow(path.getAbsolutePath)

        val flowString = fileToString(path)
        checkOutputString(flowString,
          s"""digraph {
             |  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
             |  node [shape=plain]
             |  rankdir=LR;
             |
             |  "Aggregate_x" [label=<
             |  <table border="1" cellborder="0" cellspacing="0">
             |    <tr><td bgcolor="lightgray"><i>Aggregate_x</i></td></tr>
             |    <tr><td port="0">k</td></tr>
             |  <tr><td port="1">sum(v)</td></tr>
             |  </table>>];
             |
             |  "t" [label=<
             |  <table border="1" cellborder="0" cellspacing="0">
             |    <tr><td bgcolor="lightyellow"><i>t</i></td></tr>
             |    <tr><td port="0">k</td></tr>
             |  <tr><td port="1">sum(v)</td></tr>
             |  </table>>];
             |
             |  "t" [label=<
             |  <table border="1" cellborder="0" cellspacing="0">
             |    <tr><td bgcolor="lightyellow"><i>t</i></td></tr>
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
}
