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

get_package_variables_from_pom() {
  local _ROOTT_DIR="$1"

  PACKAGE_NAME=`grep "<project.package.name>" "${_ROOTT_DIR}/pom.xml" | head -n1 | awk -F '[<>]' '{print $3}'`
  PACKAGE_VERSION=`grep "<version>" "${_ROOTT_DIR}/pom.xml" | head -n2 | tail -n1 | awk -F '[<>]' '{print $3}'`
  SCALA_VERSION=`grep "<scala.version>" "${_ROOTT_DIR}/pom.xml" | head -n1 | awk -F '[<>]' '{print $3}'`
  SCALA_BINARY_VERSION=`grep "<scala.binary.version>" "${_ROOTT_DIR}/pom.xml" | head -n1 | awk -F '[<>]' '{print $3}'`
  SPARK_VERSION=`grep "<spark.version>" "${_ROOTT_DIR}/pom.xml" | head -n1 | awk -F '[<>]' '{print $3}'`
  SPARK_BINARY_VERSION=`grep "<spark.binary.version>" "${_ROOTT_DIR}/pom.xml" | head -n1 | awk -F '[<>]' '{print $3}'`
  PACKAGE_JAR_NAME="${PACKAGE_NAME}_${SCALA_BINARY_VERSION}_spark${SPARK_BINARY_VERSION}_${PACKAGE_VERSION}-with-dependencies.jar"
}

