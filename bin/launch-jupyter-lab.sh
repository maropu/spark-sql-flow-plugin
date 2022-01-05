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

#
# Launch a JupyterLab

FWDIR="$(cd "`dirname $0`"/..; pwd)"

if [ -z "$CONDA_DISABLED" ]; then
  # Activate a conda virtual env
  . ${FWDIR}/bin/conda.sh && activate_conda_virtual_env "${FWDIR}"
fi

if [ ! -z "$JUPYTER_EXT_INIT" ]; then
  # Install Jupyter extensions
  jupyter labextension install jupyterlab-code-snippets
  jupyter labextension install @axlair/jupyterlab_vim
  # jupyter labextension install @krassowski/jupyterlab-lsp
  jupyter labextension install @kiteco/jupyterlab-kite
  # jupyter labextension install @lckr/jupyterlab_variableinspector
  jupyter lab clean
  jupyter lab build
fi

PYTHONPATH="${FWDIR}/python" JUPYTERLAB_SETTINGS_DIR=${FWDIR}/.jupyter jupyter lab
