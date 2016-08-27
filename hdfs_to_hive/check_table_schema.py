#!/usr/bin/env python
# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from utils.hdfs_helper import HDFSHelper
import commands

__author__ = 'LH Liu'


def check_table_schema_changes(hadoop_env_cmd, hive_cmd_env, table_name, database_name, schema_path):
    schema_path = HDFSHelper.read_the_only_file_from_the_path(schema_path)
    status, schema_in_file = commands.getstatusoutput('{} fs -cat {}'.format(hadoop_env_cmd, schema_path))
    if status:
        raise IOError(schema_in_file)
    schema_length_in_file = 0
    for line in str(schema_in_file).strip().split('\n'):
        if not (('Warning:'in line) or (line is '')):
            schema_length_in_file += 1
    # print schema_length_in_file
    status, schema = commands.getstatusoutput('{} -e "use {};desc {};"'.format(hive_cmd_env, database_name, table_name))
    if status:
        raise IOError(schema)
    schema_length_in_table = len(str(schema).split('\n')) - 5 - 2
    # print schema_length_in_table
    if schema_length_in_table == schema_length_in_file:
        return False
    else:
        return True
