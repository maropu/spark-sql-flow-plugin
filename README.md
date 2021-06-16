[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/maropu/spark-sql-flow-plugin/blob/master/LICENSE)
[![Build and test](https://github.com/maropu/spark-sql-flow-plugin/workflows/Build%20and%20test/badge.svg)](https://github.com/maropu/spark-sql-flow-plugin/actions?query=workflow%3A%22Build+and+test%22)

This experimental plugin enables you to easily analyze a column-level reference relationship between views registered in Spark SQL.
The feature is useful for deciding which views should be cached and which should not.

<img src="resources/graphviz_1.svg" width="800px">

Note that the diagram above shows the column-level references of temporary views that
[spark-data-repair-plugin](https://github.com/maropu/spark-data-repair-plugin) generates to repair the cells of an input table.
In the diagram, light-pink, light-yellow, and light-blue nodes represent leaf plans, temporary views, and cached plan, respectively.

## How to visualize your views

```
$ git clone https://github.com/maropu/spark-sql-flow-plugin.git
$ cd spark-sql-flow-plugin
$ ./bin/spark-shell

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.1.2
      /_/

Using Python version 3.6.8 (default, Dec 29 2018 19:04:46)
SparkSession available as 'spark'.

# Defines some views for this example
scala> sql("CREATE TABLE TestTable (key INT, value INT)")
scala> sql("CREATE TEMPORARY VIEW TestView1 AS SELECT key, SUM(value) s FROM TestTable GROUP BY key")
scala> sql("CACHE TABLE TestView1")
scala> sql("CREATE TEMPORARY VIEW TestView2 AS SELECT t.key, t.value, v.s FROM TestTable t, TestView1 v WHERE t.key = v.key")

# Generates a Graphviz-defined statement to analyze the views
scala> import org.apache.spark.sql.SQLFlow
scala> SQLFlow.printCatalogAsSQLFlow()

digraph {
  graph [pad="0.5", nodesep="0.5", ranksep="2", fontname="Helvetica"];
  node [shape=plain]
  rankdir=LR;

  "Project_13" [label=<
  <table border="1" cellborder="0" cellspacing="0">
    <tr><td bgcolor="lightgray"><i>Project_13</i></td></tr>
    <tr><td port="0">key</td></tr>
  <tr><td port="1">value</td></tr>
  <tr><td port="2">s</td></tr>
  </table>>];

  "Join_Inner_20" [label=<
  <table border="1" cellborder="0" cellspacing="0">
    <tr><td bgcolor="lightgray"><i>Join_Inner_20</i></td></tr>
    <tr><td port="0">key</td></tr>
  <tr><td port="1">value</td></tr>
  <tr><td port="2">key</td></tr>
  <tr><td port="3">s</td></tr>
  </table>>];

  "Filter_22" [label=<
  <table border="1" cellborder="0" cellspacing="0">
    <tr><td bgcolor="lightgray"><i>Filter_22</i></td></tr>
    <tr><td port="0">key</td></tr>
  <tr><td port="1">value</td></tr>
  </table>>];

  "default.testtable" [label=<
  <table border="1" cellborder="0" cellspacing="0">
    <tr><td bgcolor="lightpink"><i>default.testtable</i></td></tr>
    <tr><td port="0">key</td></tr>
  <tr><td port="1">value</td></tr>
  </table>>];

  "Filter_25" [label=<
  <table border="1" cellborder="0" cellspacing="0">
    <tr><td bgcolor="lightgray"><i>Filter_25</i></td></tr>
    <tr><td port="0">key</td></tr>
  <tr><td port="1">s</td></tr>
  </table>>];

  "testview1" [label=<
  <table border="1" cellborder="0" cellspacing="0">
    <tr><td bgcolor="lightblue"><i>testview1</i></td></tr>
    <tr><td port="0">key</td></tr>
  <tr><td port="1">s</td></tr>
  </table>>];

  "testview2" [label=<
  <table border="1" cellborder="0" cellspacing="0">
    <tr><td bgcolor="lightyellow"><i>testview2</i></td></tr>
    <tr><td port="0">key</td></tr>
  <tr><td port="1">value</td></tr>
  <tr><td port="2">s</td></tr>
  </table>>];

  "Aggregate_28" [label=<
  <table border="1" cellborder="0" cellspacing="0">
    <tr><td bgcolor="lightgray"><i>Aggregate_28</i></td></tr>
    <tr><td port="0">key</td></tr>
  <tr><td port="1">s</td></tr>
  </table>>];

  "default.testtable" [label=<
  <table border="1" cellborder="0" cellspacing="0">
    <tr><td bgcolor="lightpink"><i>default.testtable</i></td></tr>
    <tr><td port="0">key</td></tr>
  <tr><td port="1">value</td></tr>
  </table>>];

  "testview1" [label=<
  <table border="1" cellborder="0" cellspacing="0">
    <tr><td bgcolor="lightyellow"><i>testview1</i></td></tr>
    <tr><td port="0">key</td></tr>
  <tr><td port="1">s</td></tr>
  </table>>];

  "Join_Inner_20":0 -> "Project_13":0;
  "Join_Inner_20":1 -> "Project_13":1;
  "Join_Inner_20":3 -> "Project_13":2;
  "Filter_22":0 -> "Join_Inner_20":0;
  "Filter_22":1 -> "Join_Inner_20":1;
  "Filter_25":0 -> "Join_Inner_20":2;
  "Filter_25":1 -> "Join_Inner_20":3;
  "default.testtable":0 -> "Filter_22":0;
  "default.testtable":1 -> "Filter_22":1;
  "testview1":0 -> "Filter_25":0;
  "testview1":1 -> "Filter_25":1;
  "Project_13":0 -> "testview2":0;
  "Project_13":1 -> "testview2":1;
  "Project_13":2 -> "testview2":2;
  "default.testtable":0 -> "Aggregate_28":0;
  "default.testtable":1 -> "Aggregate_28":1;
  "Aggregate_28":0 -> "testview1":0;
  "Aggregate_28":1 -> "testview1":1;
}

```

You can use the Graphviz `dot` command or [GraphvizOnline](https://dreampuf.github.io/GraphvizOnline) to
convert the generated statement into a image-formatted file, e.g., SVG and PNG.
Finally, a diagram generated from the statement is as follows:

<img src="resources/graphviz_2.svg" width="800px">

## Bug reports

If you hit some bugs and requests, please leave some comments on [Issues](https://github.com/maropu/spark-sql-flow-plugin/issues)
or Twitter([@maropu](http://twitter.com/#!/maropu)).

