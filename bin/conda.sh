#!/usr/bin/env bash

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

activate_conda_virtual_env() {
  local _ROOTT_DIR="$1"

  # Loads some variables from pom.xml
  . ${_ROOTT_DIR}/bin/package.sh "${_ROOTT_DIR}"

  # Creates a virtual env to resolve Python dependencies
  CONDA_COMMAND=${_ROOTT_DIR}/bin/conda.py
  CONDA_ENV_ID=`grep "<project.package.name>" "${_ROOTT_DIR}/pom.xml" | head -n1 | awk -F '[<>]' '{print $3}'`
  ${CONDA_COMMAND} --command=create_env_only --env_id=${CONDA_ENV_ID}
  echo -ne "\n=== Launching pyspark on conda virtual env '$(${CONDA_COMMAND} --command=get_env_name --env_id=${CONDA_ENV_ID})' ===\n"

  # Gets virtual env home
  CONDA_ENV_HOME=$(${CONDA_COMMAND} --command=get_env_home --env_id=${CONDA_ENV_ID})

  # Then, activates the virtual env
  ACTIVATE_CONDA_ENV=$(${CONDA_COMMAND} --command=get_activate_command --env_id=${CONDA_ENV_ID})
  eval "${ACTIVATE_CONDA_ENV}"
}

