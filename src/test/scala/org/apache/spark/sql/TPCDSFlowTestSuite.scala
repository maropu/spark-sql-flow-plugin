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

import org.apache.spark.sql.catalyst.TableIdentifier

class TPCDSFlowTestSuite extends SQLFlowTestSuite with TPCDSSchema {

  private val tableNames: Iterable[String] = tableColumns.keys

  private def createTable(spark: SparkSession, tableName: String): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE `$tableName` (${tableColumns(tableName)})
         |USING parquet
       """.stripMargin)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    tableNames.foreach { tableName =>
      createTable(spark, tableName)
    }
  }

  override def afterAll(): Unit = {
    tableNames.foreach { tableName =>
      spark.sessionState.catalog.dropTable(TableIdentifier(tableName), true, true)
    }
    super.afterAll()
  }

  override protected def baseResourcePath = {
    getWorkspaceFilePath("src", "test", "resources", "tpcds-flow-tests").toFile
  }

  override protected def ignoreList: Set[String] = Set.empty

  override protected def runQuery(query: String, session: SparkSession): Unit = {
    session.sql(query).createOrReplaceTempView("v")
  }
}
