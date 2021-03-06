#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import re
import unittest
from pyspark import SparkConf
from pyspark.sql import Row, functions as f

from sqlflow import *
from tests.testutils import ReusedSQLTestCase


class SQLFlowTests(ReusedSQLTestCase):

    @classmethod
    def conf(cls):
        return SparkConf() \
            .set("spark.master", "local[*]") \
            .set("spark.driver.memory", "4g") \
            .set("spark.jars", os.getenv("SQLFLOW_LIB"))

    def setUp(self):
        super(ReusedSQLTestCase, self).setUp()
        assert(self.spark.sql('SHOW VIEWS').count() == 0)

    def tearDown(self):
        super(ReusedSQLTestCase, self).tearDown()
        views = map(lambda r: r.viewName, filter(lambda r: r.isTemporary, self.spark.sql('SHOW VIEWS').collect()))
        for v in views:
            self.spark.catalog.dropTempView(v)

    def _debug_print_as_sqlflow(self, graph_format: str, options: str = '') -> str:
        jvm = self.spark.sparkContext._active_spark_context._jvm
        return jvm.SQLFlowApi.toSQLFlowString(False, graph_format, options)

    def _test_generated_graphviz_edges(self, expected: List[str]) -> None:
        sqlflow = re.sub('_[0-9]+":', '_X":', self._debug_print_as_sqlflow('graphviz'))
        sqlflow = re.sub('_[a-z0-9]{7}":', '_X":', sqlflow)
        edges = re.findall(r'"[a-zA-Z_]+":\d -> "[a-zA-Z_]+":\d', sqlflow)
        self.assertEqual(set(edges), set(expected))

    def _test_generated_adj_list_edges(self, expected: List[str]) -> None:
        sqlflow = re.sub('_[a-z0-9]{7}', '_X', self._debug_print_as_sqlflow('adjacency_list', 'sep=:'))
        sqlflow = re.sub('_[0-9]{1}', '_X', sqlflow)
        edges = re.findall(r'[a-zA-Z0-9_]+:[a-zA-Z0-9_]+', sqlflow)
        self.assertEqual(set(edges), set(expected))

    def test_basics(self):
        @auto_tracking
        def transform_alpha(df):
            return df.selectExpr('id % 3 AS key', 'id % 5 AS value')

        @auto_tracking_with('transform_delta')
        def transform_beta(df):
            return df.groupBy('key').agg(f.expr('collect_set(value)').alias('value'))

        @auto_tracking
        def transform_gamma(df):
            return df.selectExpr('explode(value)')

        @auto_tracking
        def transform_lambda(df):
            return df.count()

        # Applies a chain of transformation functions
        cnt = transform_lambda(transform_gamma(transform_beta(transform_alpha(self.spark.range(3)))))
        self.assertEqual(cnt, 3)

        self._test_generated_graphviz_edges([
            '"Aggregate_X":0 -> "transform_delta":0',
            '"Project_X":1 -> "transform_alpha":1',
            '"transform_delta":0 -> "Filter_X":0',
            '"transform_alpha":0 -> "Aggregate_X":0',
            '"Range_X":0 -> "Project_X":0',
            '"transform_delta":1 -> "Filter_X":1',
            '"Range_X":0 -> "Project_X":1',
            '"Aggregate_X":1 -> "transform_delta":1',
            '"Project_X":0 -> "transform_alpha":0',
            '"Project_X":0 -> "Generate_X":0',
            '"transform_alpha":1 -> "Aggregate_X":1',
            '"Filter_X":1 -> "Project_X":0',
            '"Generate_X":0 -> "transform_lambda":0',
            '"transform_lambda":0 -> "transform_gamma":0'])

        self._test_generated_adj_list_edges([
            'Range_X:Project_X',
            'transform_lambda:transform_gamma',
            'Aggregate_X:transform_delta',
            'transform_delta:Filter_X',
            'transform_alpha:Aggregate_X',
            'Project_X:Generate_X',
            'Generate_X:transform_lambda',
            'Project_X:transform_alpha',
            'Filter_X:Project_X'])

    def test_list_case(self):
        @auto_tracking
        def transform_alpha(df):
            df1 = df.selectExpr('id % 3 AS v')
            df2 = df.selectExpr('id % 5 AS v')
            return [df1, df2]  # list

        @auto_tracking
        def transform_beta(dfs):
            import functools
            df = functools.reduce(lambda x, y: x.union(y), dfs)
            return df.distinct()

        # Applies a chain of transformation functions
        df = transform_beta(transform_alpha(self.spark.range(5)))
        self.assertEqual(df.orderBy('v').collect(), [Row(v=0), Row(v=1), Row(v=2), Row(v=3), Row(v=4)])

        self._test_generated_graphviz_edges([
            '"Range_X":0 -> "Project_X":0',
            '"transform_alpha_X":0 -> "Union_X":0',
            '"Project_X":0 -> "transform_alpha":0',
            '"Aggregate_X":0 -> "transform_beta":0',
            '"transform_alpha":0 -> "Union_X":0',
            '"Union_X":0 -> "Aggregate_X":0',
            '"Project_X":0 -> "transform_alpha_X":0'])

        self._test_generated_adj_list_edges([
            'Range_X:Project_X',
            'transform_alpha:Union_X',
            'Aggregate_X:transform_beta',
            'Project_X:transform_alpha_X',
            'Project_X:transform_alpha',
            'Union_X:Aggregate_X',
            'transform_alpha_X:Union_X'])

    def test_tuple_case(self):
        @auto_tracking
        def transform_alpha(df):
            df1 = df.selectExpr('id % 3 AS v')
            df2 = df.selectExpr('id % 5 AS v')
            return df1, df2  # tuple

        @auto_tracking
        def transform_beta(dfs):
            import functools
            df = functools.reduce(lambda x, y: x.union(y), dfs)
            return df.distinct()

        # Applies a chain of transformation functions
        df = transform_beta(transform_alpha(self.spark.range(5)))
        self.assertEqual(df.orderBy('v').collect(), [Row(v=0), Row(v=1), Row(v=2), Row(v=3), Row(v=4)])

        self._test_generated_graphviz_edges([
            '"Project_X":0 -> "transform_alpha_X":0',
            '"transform_alpha":0 -> "Union_X":0',
            '"Project_X":0 -> "transform_alpha":0',
            '"Union_X":0 -> "Aggregate_X":0',
            '"Aggregate_X":0 -> "transform_beta":0',
            '"transform_alpha_X":0 -> "Union_X":0',
            '"Range_X":0 -> "Project_X":0'])

        self._test_generated_adj_list_edges([
            'Range_X:Project_X',
            'transform_alpha:Union_X',
            'Aggregate_X:transform_beta',
            'Project_X:transform_alpha_X',
            'Project_X:transform_alpha',
            'Union_X:Aggregate_X',
            'transform_alpha_X:Union_X'])

    def test_dict_case(self):
        @auto_tracking
        def transform_alpha(df):
            df1 = df.selectExpr('id % 3 AS v')
            df2 = df.selectExpr('id % 5 AS v')
            return {'df1': df1, 'df2': df2}  # dict

        @auto_tracking
        def transform_beta(dfs):
            import functools
            df = functools.reduce(lambda x, y: x.union(y), dfs.values())
            return df.distinct()

        # Applies a chain of transformation functions
        df = transform_beta(transform_alpha(self.spark.range(5)))
        self.assertEqual(df.orderBy('v').collect(), [Row(v=0), Row(v=1), Row(v=2), Row(v=3), Row(v=4)])

        self._test_generated_graphviz_edges([
            '"Project_X":0 -> "transform_alpha_X":0',
            '"transform_alpha":0 -> "Union_X":0',
            '"Project_X":0 -> "transform_alpha":0',
            '"Union_X":0 -> "Aggregate_X":0',
            '"Aggregate_X":0 -> "transform_beta":0',
            '"transform_alpha_X":0 -> "Union_X":0',
            '"Range_X":0 -> "Project_X":0'])

        self._test_generated_adj_list_edges([
            'Range_X:Project_X',
            'transform_alpha:Union_X',
            'Aggregate_X:transform_beta',
            'Project_X:transform_alpha_X',
            'Project_X:transform_alpha',
            'Union_X:Aggregate_X',
            'transform_alpha_X:Union_X'])


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
