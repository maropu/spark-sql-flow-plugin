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
import unittest
from pyspark import SparkConf

import sqlflow
from neo4j import GraphDatabase
from tests.testutils import ReusedSQLTestCase


# Checks if the params to access a Neo4j Aura database provided
env = dict(os.environ)
neo4j_integration_tests_disabled = False

neo4jaura_params = ['NEO4J_AURADB_URI', 'NEO4J_AURADB_USER', 'NEO4J_AURADB_PASSWD']
if not all(map(lambda p: p in env, neo4jaura_params)):
    neo4j_integration_tests_disabled = True


class Neo4jAuraDb:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run(self, query):
        with self.driver.session() as session:
            return session.read_transaction(self._run_query, query)

    @staticmethod
    def _run_query(tx, query):
        return tx.run(query).data()


@unittest.skipIf(
    neo4j_integration_tests_disabled,
    "envs {} must be provided to enable this test".format(', '.join(map(lambda p: f"'{p}'", neo4jaura_params))))
class Neo4jAuraSinkTests(ReusedSQLTestCase):

    @classmethod
    def conf(cls):
        return SparkConf() \
            .set("spark.master", "local[*]") \
            .set("spark.driver.memory", "4g") \
            .set("spark.sql.catalogImplementation", "hive") \
            .set("spark.jars", os.getenv("SQLFLOW_LIB"))

    def setUp(self):
        super(ReusedSQLTestCase, self).setUp()
        assert(self.spark.sql('SHOW VIEWS').count() == 0)

    def tearDown(self):
        super(ReusedSQLTestCase, self).tearDown()
        views = map(lambda r: r.viewName, filter(lambda r: r.isTemporary, self.spark.sql('SHOW VIEWS').collect()))
        for v in views:
            self.spark.catalog.dropTempView(v)

    def test_basics(self):
        with self.table("TestTable"):
            self.spark.sql("CREATE TABLE TestTable (key INT, value INT)")

            with self.tempView("TestTable1", "TestTable2"):
                self.spark.sql("CREATE TEMPORARY VIEW TestView1 AS SELECT key, SUM(value) s "
                               "FROM TestTable GROUP BY key")
                self.spark.sql("CREATE TEMPORARY VIEW TestView2 AS SELECT t.key, t.value, v.s "
                               "FROM TestTable t, TestView1 v WHERE t.key = v.key")

                neo4j_uri = os.getenv("NEO4J_AURADB_URI")
                neo4j_user = os.getenv("NEO4J_AURADB_USER")
                neo4j_passwd = os.getenv("NEO4J_AURADB_PASSWD")
                neo4jaura_options = {"uri": neo4j_uri, "user": neo4j_user, "passwd": neo4j_passwd, "overwrite": "true"}
                sqlflow.export_data_lineage_into(
                    "neo4jaura", contracted=False, options=neo4jaura_options)

                neo4jdb = Neo4jAuraDb(neo4j_uri, neo4j_user, neo4j_passwd)
                result = neo4jdb.run('MATCH(n) RETURN n.name AS name')
                self.assertEqual(set(map(lambda r: r["name"], result)), set([
                    'Filter', 'Project', 'testview2', 'default.testtable', 'testview1', 'Join_Inner', 'Aggregate']))


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
