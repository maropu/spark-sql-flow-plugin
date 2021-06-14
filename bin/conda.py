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

"""
Prepares a conda virtual env for PySpark
"""

import hashlib
import json
import os
import subprocess
import sys

from argparse import ArgumentParser

# Environment variable indicating a path to a conda installation.
# This script will default to running "conda" if unset
PYSPARK_CONDA_HOME = "PYSPARK_CONDA_HOME"


class ShellCommandException(Exception):
    pass


def _get_conda_env_name(conda_env_path, env_name_prefix):
    conda_env_contents = open(conda_env_path).read() if conda_env_path else ""
    return "{}-{}".format(env_name_prefix,
                          hashlib.sha1(conda_env_contents.encode("utf-8")).hexdigest())


def _exec_cmd(cmd, throw_on_error=True, env=None, stream_output=False, cwd=None, cmd_stdin=None,
              **kwargs):
    """
    Runs a command as a child process.

    A convenience wrapper for running a command from a Python script.
    Keyword arguments:
    cmd -- the command to run, as a list of strings
    throw_on_error -- if true, raises an Exception if the exit code of the program is nonzero
    env -- additional environment variables to be defined when running the child process
    cwd -- working directory for child process
    stream_output -- if true, does not capture standard output and error; if false, captures these
      streams and returns them
    cmd_stdin -- if specified, passes the specified string as stdin to the child process.

    Note on the return value: If stream_output is true, then only the exit code is returned. If
    stream_output is false, then a tuple of the exit code, standard output and standard error is
    returned.
    """
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    if stream_output:
        child = subprocess.Popen(cmd, env=cmd_env, cwd=cwd, universal_newlines=True,
                                 stdin=subprocess.PIPE, **kwargs)
        child.communicate(cmd_stdin)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise ShellCommandException("Non-zero exitcode: {}".format(exit_code))
        return exit_code
    else:
        child = subprocess.Popen(
            cmd, env=cmd_env, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE,
            cwd=cwd, universal_newlines=True, **kwargs)
        (stdout, stderr) = child.communicate(cmd_stdin)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise ShellCommandException("Non-zero exit code: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                                        (exit_code, stdout, stderr))
        return exit_code, stdout, stderr


def _get_conda_bin_executable(executable_name):
    """
    Return path to the specified executable, assumed to be discoverable within the 'bin'
    subdirectory of a conda installation.

    The conda home directory (expected to contain a 'bin' subdirectory) is configurable via the
    ``PYSPARK_CONDA_HOME`` environment variable. If
    ``PYSPARK_CONDA_HOME`` is unspecified, this method simply returns the passed-in
    executable name.
    """
    conda_home = os.environ.get(PYSPARK_CONDA_HOME)
    if conda_home:
        return os.path.join(conda_home, "bin/{}".format(executable_name))
    return executable_name


def _get_conda_path():
    conda_path = _get_conda_bin_executable("conda")
    try:
        _exec_cmd([conda_path, "--help"], throw_on_error=False)
    except EnvironmentError:
        raise RuntimeError("Could not find Conda executable. "
                           "Ensure Conda is installed as per the instructions "
                           "at https://conda.io/docs/user-guide/install/index.html.")
    return conda_path


def _get_or_create_conda_env(conda_env_path, env_name_prefix):
    """
    Creates a conda environment containing the dependencies if such a conda environment
    doesn't already exist. Returns the name of the conda environment.
    """
    conda_path = _get_conda_path()
    (_, stdout, _) = _exec_cmd([conda_path, "env", "list", "--json"])
    env_names = [os.path.basename(env) for env in json.loads(stdout)['envs']]
    project_env_name = _get_conda_env_name(conda_env_path, env_name_prefix)
    if project_env_name not in env_names:
        if conda_env_path:
            _exec_cmd([conda_path, "env", "create", "-n", project_env_name, "--file",
                       conda_env_path], stream_output=True)
        else:
            _exec_cmd([conda_path, "create", "-n", project_env_name, "python"], stream_output=True)
    return project_env_name


def _get_conda_command(conda_env_name):
    activate_path = _get_conda_bin_executable("activate")
    # in case os name is not 'nt', we are not running on windows. It introduces
    # bash command otherwise.
    if os.name != "nt":
        return "source {} {}".format(activate_path, conda_env_name)
    else:
        return "conda {} {}".format(activate_path, conda_env_name)


def _get_conda_env_home(conda_env_name):
    conda_path = _get_conda_path()
    (_, stdout, _) = _exec_cmd([conda_path, "env", "list", "--json"])
    env_names = json.loads(stdout)['envs']
    for env_home in env_names:
        if os.path.basename(env_home) == conda_env_name:
            return env_home

    raise RuntimeError(f"Could not find Conda home for '{conda_env_name}'")


if __name__ == "__main__":

    # Parses command-line arguments
    parser = ArgumentParser()
    parser.add_argument('--command', dest='command', type=str, required=True)
    parser.add_argument('--env_id', dest='env_id', type=str, required=True)
    args = parser.parse_args()

    conda_env_path = os.path.dirname(__file__) + '/conda.yml'
    conda_env_name = _get_or_create_conda_env(conda_env_path, args.env_id)

    if args.command == 'get_activate_command':
        command = _get_conda_command(conda_env_name)
        print(command)
    elif args.command == 'get_env_home':
        conda_env_home = _get_conda_env_home(conda_env_name)
        print(conda_env_home)
    elif args.command == 'get_env_name':
        print(conda_env_name)
    elif args.command == 'create_env_only':
        print("conda virtual env '{}' created.".format(conda_env_name))
    else:
        print("Unknown command: {}".format(args.command))
        sys.exit(-1)
