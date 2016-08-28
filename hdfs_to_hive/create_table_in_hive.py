#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from utils.hdfs_helper import HDFSHelper
import commands
import logging
import os

__author__ = 'LH Liu'

logger = logging.getLogger()


def create_table(hadoop_cmd_env, hive_cmd_env, hive_store_folder_path, table_name, table_info_detail):
    # init HDFSHelper.hadoop_cmd_env
    HDFSHelper.hadoop_cmd_env = hadoop_cmd_env
    # get full schema path: there are 2 conditions: 1.path is a folder 2.schema is already a file
    incomplete_schema_path = table_info_detail.get('schema').strip("'")
    schema_path = HDFSHelper.read_the_only_file_from_the_path(incomplete_schema_path)
    # get schema
    status, schema = commands.getstatusoutput('{} fs -cat {} '.format(hadoop_cmd_env, schema_path))
    if status:
        raise IOError(schema)
    schema_list = []
    for line in str(schema).strip().split('\n'):
        if not (('Warning:' in line) or (line is '')):
            schema_list.append(
                line.replace(',', ':').replace(';', ':').replace(
                    ':', ' '
                ).replace(' long', ' bigint').replace(' chararray', ' string')
            )
    schema = ','.join(schema_list).strip().strip(',')
    # get location
    database_path_name = table_info_detail.get('database', 'external_table_data')
    if database_path_name not in ('XXX', 'default'):
        database_path_name += '.db'
    else:
        database_path_name = 'external_table_data'
    table_path = os.path.join(hive_store_folder_path, database_path_name, table_name)
    HDFSHelper.make_dir_in_hdfs(table_path)
    # create table
    database_name = table_info_detail.get('database', 'default')
    status, output = commands.getstatusoutput(
        '{} -e "use {};create external table if not exists {}({}) '.format(hive_cmd_env,
                                                                           database_name, table_name, schema)
        + 'row format delimited fields terminated by \'\\t\' stored as textfile ' +
        'location \'hdfs://master:9000{}\';"'.format(table_path)
    )
    if status:
        raise IOError(output)
