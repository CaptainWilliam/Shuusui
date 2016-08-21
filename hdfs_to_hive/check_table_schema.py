#!/usr/bin/env python
# -*- coding:utd-8 -*-
from __future__ import unicode_literals
from utils.hdfs_helper import HDFSHelper
import commands

__author__ = 'LH Liu'


def check_table_schema(hadoop_env_cmd, hive_env_cmd, table_name, schema_path):
    schema_path = HDFSHelper.read_the_only_file_from_the_path(schema_path)
    status, schema_in_file = commands.getstatusoutput('{} fs -cat {}'.format(hadoop_env_cmd, schema_path))
    if status:
        raise IOError(schema_in_file)
    schema_length_in_file = 0
    for line in str(schema_in_file).split('\n'):
        if not (('Warning:'in line) or (line is '')):
            schema_length_in_file += 1
    status, schema_in_table = commands.getstatusoutput('{} -e "desc {};"'.format(hive_env_cmd, table_name))
    if status:
        raise IOError(schema_in_table)
    schema_length_in_table = len(str(schema_in_table).split('\n'))
    if schema_length_in_table == schema_length_in_file:
        return True
    else:
        return False



