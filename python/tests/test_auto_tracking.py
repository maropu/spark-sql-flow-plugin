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

from auto_tracking import *
from tests.testutils import ReusedSQLTestCase


class AutoTrackingTests(ReusedSQLTestCase):

    @classmethod
    def conf(cls):
        return SparkConf() \
            .set("spark.master", "local[*]") \
            .set("spark.driver.memory", "4g") \
            .set("spark.jars", os.getenv("SQLFLOW_LIB"))

    @classmethod
    def setUpClass(cls):
        super(AutoTrackingTests, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(ReusedSQLTestCase, cls).tearDownClass()

    def _debug_print_as_sqlflow(self, contracted: bool = False) -> str:
        jvm = self.spark.sparkContext._active_spark_context._jvm
        return jvm.SQLFlowApi.toSQLFlowString(contracted)

    def test_basics(self):
        @auto_tracking
        def transform_alpha(df):
            return df.selectExpr('id % 3 AS key', 'id % 5 AS value')

        @auto_tracking
        def transform_beta(df):
            return df.groupBy('key').agg(f.expr('collect_set(value)').alias('value'))

        @auto_tracking
        def transform_gamma(df):
            return df.selectExpr('explode(value)')

        # Applies a chain of transformation functions
        df = transform_gamma(transform_beta(transform_alpha(self.spark.range(3))))
        self.assertEqual(df.collect(), [Row(col=0), Row(col=1), Row(col=2)])

        sqlflow = re.sub('_\d+', '_X', self._debug_print_as_sqlflow())
        edges = re.findall(r'"[a-zA-Z_]+":\d -> "[a-zA-Z_]+":\d;', sqlflow)
        self.assertEqual(set(edges), set([
            '"Aggregate_X":0 -> "transform_beta":0;',
            '"Aggregate_X":1 -> "transform_beta":1;',
            '"Filter_X":1 -> "Project_X":0;',
            '"Generate_X":0 -> "transform_gamma":0;',
            '"Project_X":0 -> "transform_alpha":0;',
            '"Project_X":1 -> "transform_alpha":1;',
            '"Range_X":0 -> "Project_X":0;',
            '"Range_X":0 -> "Project_X":1;',
            '"transform_alpha":0 -> "Aggregate_X":0;',
            '"transform_alpha":1 -> "Aggregate_X":1;',
            '"transform_beta":0 -> "Filter_X":0;',
            '"transform_beta":1 -> "Filter_X":1;']))


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
