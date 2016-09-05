#!/usr/bin/env python
# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from utils.hdfs_helper import HDFSHelper
import commands
import re

__author__ = 'LH Liu'


def check_table_schema_changes(hadoop_env_cmd, hive_cmd_env, table_name, database_name, schema_path):
    schema_path = HDFSHelper.read_the_only_file_from_the_path(schema_path)
    status, schema_in_hdfs = commands.getstatusoutput('{} fs -cat {}'.format(hadoop_env_cmd, schema_path))
    if status:
        raise IOError(schema_in_hdfs)
    hdfs_schema_list = []
    for line in str(schema_in_hdfs).strip().split('\n'):
        if not (('Warning:' in line) or (line is '')):
            changed_format_line = line.replace(',', ':').replace(';', ':').replace(':', ' ').replace(
                ' long', ' bigint').replace(' chararray', ' string')
            hdfs_schema_list.append(changed_format_line)
    schema_length_in_hdfs = len(hdfs_schema_list)
    # print schema_length_in_file
    status, schema = commands.getstatusoutput('{} -e "use {};desc {};"'.format(hive_cmd_env, database_name, table_name))
    if status:
        raise IOError(schema)
    hive_schema_list = []
    for line in str(schema).split('\n')[6:-1]:
        if not (('Warning:' in line) or (line is '')):
            changed_format_line = ' '.join(re.split(r'\s+', line.strip()))
            hive_schema_list.append(changed_format_line)
    schema_length_in_hive = len(hive_schema_list)
    # print schema_length_in_table
    if schema_length_in_hive == schema_length_in_hdfs:
        for i in range(0, schema_length_in_hdfs):
            if str(hdfs_schema_list[i]).strip() != str(hive_schema_list[i]).strip():
                return True
        return False
    else:
        return True
