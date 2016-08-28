#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from utils.config_helper import ConfigHelper
from utils.hdfs_helper import HDFSHelper
from check_table_schema import check_table_schema_changes
from create_table_in_hive import create_table
from update_table_in_hive import update_table
import commands
import logging
import os

__author__ = 'LH Liu'

logger = logging.getLogger()


def process(hadoop_cmd_env, hive_cmd_env, hive_conf_path, hive_store_folder_path):
    # init HDFSHelper.hadoop_cmd_env
    HDFSHelper.hadoop_cmd_env = hadoop_cmd_env

    # 1.load conf
    first_table_info = ConfigHelper.load_config(hive_conf_path)

    # 2 process every table(hdfs_to_hive)
    for table_name, table_info_detail in first_table_info.items():
        # if path does not exist, create table
        database_path_name = table_info_detail.get('database', 'external_table_data')
        if database_path_name not in ('external_table_data', 'default'):
            database_path_name += '.db'
        else:
            database_path_name = 'external_table_data'
        table_store_path = os.path.join(hive_store_folder_path, database_path_name, table_name)
        try:
            # check path to find out whether table exists
            HDFSHelper.check_hdfs_path(table_store_path)
        except Exception, e:
            logger.info(e.message + ' And create table now!')
            try:
                create_table(hadoop_cmd_env, hive_cmd_env, hive_store_folder_path, table_name, table_info_detail)
            except Exception, e:
                logger.info(e.message)

        # check is table schema changed
        database_name = table_info_detail.get('database', 'default')
        schema_path = table_info_detail.get('schema')
        schema_changed = False
        try:
            schema_changed = check_table_schema_changes(hadoop_cmd_env, hive_cmd_env,
                                                        table_name, database_name, schema_path)
        except Exception, e:
            logger.info(e.message + ' Check schema changes failed!')
        if schema_changed:
            try:
                commands.getstatusoutput('{} -e "use {};drop {};"'.format(hive_cmd_env, database_name, table_name))
            except Exception, e:
                logger.info(e.message)
            try:
                create_table(hadoop_cmd_env, hive_cmd_env, hive_store_folder_path, table_name, table_info_detail)
            except Exception, e:
                logger.info(e.message)

        # after table was created, update it
        try:
            update_table(hadoop_cmd_env, hive_store_folder_path, table_name, table_info_detail)
        except Exception, e:
            logger.errer(e.message)
