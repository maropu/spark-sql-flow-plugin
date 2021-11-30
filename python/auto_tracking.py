#!/usr/bin/env python3

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

import functools
import shutil
import uuid
from pyspark.sql import DataFrame, SparkSession
from typing import Any


def _setup_logger() -> Any:
    from logging import getLogger, NullHandler, INFO
    logger = getLogger(__name__)
    logger.setLevel(INFO)
    logger.addHandler(NullHandler())
    return logger


_logger = _setup_logger()


def _create_temp_name(prefix: str = "temp") -> str:
    return f'{prefix}_{uuid.uuid4().hex.lower()[:7]}'


def _check_if_table_exists(ident: str) -> bool:
    try:
        SparkSession.getActiveSession().table(ident)
        return True
    except Exception as e:
        return False


def auto_tracking(f):  # type: ignore
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):  # type: ignore
        # If ret is `DataFrame`, creates a temp table for tracking
        # transformation process.
        ret = f(self, *args, **kwargs)
        if type(ret) is DataFrame:
            _logger.info(f'Automatically tracking: {f.__name__}({",".join(ret.columns)})')
            if _check_if_table_exists(f.__name__):
                ret.createOrReplaceTempView(_create_temp_name(f.__name__))
            else:
                ret.createTempView(f.__name__)

        return ret

    return wrapper


def save_data_lineage(output_dir_path: str, filename_prefix: str = "sqlflow", format: str = "svg",
                      contracted: bool = False, overwrite: bool = False) -> None:
    try:
        jvm = SparkSession.builder.getOrCreate().sparkContext._active_spark_context._jvm  # type: ignore
        jvm.SQLFlowApi.saveAsSQLFlow(output_dir_path, filename_prefix, format, contracted, overwrite)
    except:
        _logger.warning(f'Failed to save data lineage in {output_dir_path}')
