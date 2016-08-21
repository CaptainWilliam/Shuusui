#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from utils.config_helper import ConfigHelper
from utils.hdfs_helper import HDFSHelper
from check_table_schema import check_table_schema
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
        table_store_path = os.path.join(hive_store_folder_path, table_name)
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
        if not check_table_schema(hadoop_cmd_env, hadoop_cmd_env, table_name, table_info_detail.get('schema')):
            commands.getstatusoutput('{} -e "drop {};"'.format(hive_cmd_env, table_name))
            create_table(hadoop_cmd_env, hive_cmd_env, hive_store_folder_path, table_name, table_info_detail)

        # after table was created, update it
        update_table(hadoop_cmd_env, hive_store_folder_path, table_name, table_info_detail)
