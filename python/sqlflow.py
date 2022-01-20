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
import inspect
import os
import uuid
from pyspark.sql import DataFrame, SparkSession
from typing import Any, Dict, List


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


def _extract_dataframes_from(ret: Any) -> List[DataFrame]:
    if type(ret) is DataFrame:
        return [ret]
    if type(ret) in (list, tuple):
        return [e for e in ret if type(e) is DataFrame]
    if type(ret) is dict:
        return [e for e in ret.values() if type(e) is DataFrame]

    return []


def _create_tracking_view(df: DataFrame, name: str) -> str:
    if _check_if_table_exists(name):
        new_name = _create_temp_name(name)
        df.createOrReplaceTempView(new_name)
        return new_name
    else:
        df.createTempView(name)
        return name


def _create_tracking_views(dfs: List[DataFrame], name: str) -> None:
    # If ret holds `DataFrame`s, creates temp tables for tracking
    # transformation process.
    for df in dfs:
        ident = _create_tracking_view(df, name)
        _logger.info(f'Automatically tracking: {ident}({",".join(df.columns)})')


def _auto_tracking_enabled() -> bool:
    return os.environ.get("SQLFLOW_AUTO_TRACKING_DISABLED") is None


def auto_tracking(f):  # type: ignore
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):  # type: ignore
        ret = f(self, *args, **kwargs)

        if _auto_tracking_enabled():
            # If ret holds `DataFrame`s, creates temp tables for tracking
            # transformation process.
            output_dfs = _extract_dataframes_from(ret)
            if not output_dfs:
                input_values = [v for v in inspect.signature(f).bind(self, *args, **kwargs).arguments.values()]
                _create_tracking_views(_extract_dataframes_from(input_values), f.__name__)
            else:
                _create_tracking_views(output_dfs, f.__name__)

        return ret
    return wrapper


def auto_tracking_with(name):  # type: ignore
    def _auto_tracking(f):  # type: ignore
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):  # type: ignore
            ret = f(self, *args, **kwargs)

            # If ret holds `DataFrame`s, creates temp tables for tracking
            # transformation process.
            output_dfs = _extract_dataframes_from(ret)
            if not output_dfs:
                input_values = [v for v in inspect.signature(f).bind(self, *args, **kwargs).arguments.values()]
                _create_tracking_views(_extract_dataframes_from(input_values), name)
            else:
                _create_tracking_views(output_dfs, name)

            return ret
        return wrapper
    return _auto_tracking


def save_data_lineage(output_dir_path: str, filename_prefix: str = "sqlflow", graph_sink: str = "graphviz",
                      contracted: bool = False, overwrite: bool = False) -> None:
    jvm = SparkSession.builder.getOrCreate().sparkContext._active_spark_context._jvm  # type: ignore
    options = f'outputDirPath={output_dir_path},filenamePrefix={filename_prefix},overwrite={overwrite}'
    jvm.SQLFlowApi.saveAsSQLFlow(graph_sink, contracted, options)


def export_data_lineage_into(graph_sink: str, contracted: bool = False, options: Dict[str, str] = {}) -> None:
    jvm = SparkSession.builder.getOrCreate().sparkContext._active_spark_context._jvm  # type: ignore
    options_as_string = ','.join(map(lambda kv: f'{kv[0]}={kv[1]}', options.items()))
    jvm.SQLFlowApi.exportSQLFlowInto(graph_sink, contracted, options_as_string)
