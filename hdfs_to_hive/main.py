#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from utils.config_helper import ConfigHelper
from process import process
import logging.config
import logging
import argparse
import os

__author__ = 'LH Liu'


def set_logging(log_folder_path, log_level):
    if not os.path.exists(log_folder_path):
        os.makedirs(log_folder_path)
    # set log file path
    log_file_name = 'log'
    log_file_path = os.path.join(log_folder_path, log_file_name)
    log_config = ConfigHelper.load_config(os.path.join(os.path.dirname(__file__), 'conf', 'log.yaml'))
    log_config['handlers']['file_handler']['filename'] = log_file_path
    log_config['handlers']['file_handler']['level'] = log_level
    logging.config.dictConfig(log_config)


def main():
    # read parameters
    parser = argparse.ArgumentParser(prog='Hdfs source to hive')
    log_parameter = parser.add_argument_group('Log parameters', 'Parameters are used for logging.')
    log_parameter.add_argument('--log_folder_path', default='logs', help='The log output position.')
    log_parameter.add_argument('--log_level', default='DEBUG',
                               choices=['DEBUG', 'INFO', 'ERROR', 'WARNING', 'CRITICAL'], help='The log level.')
    group_call = parser.add_argument_group('Group call', 'Parameters are used for calling this plugin.')
    group_call.add_argument('--hadoop_cmd_env', default='/usr/local/hadoop/bin/hadoop',
                            help='The hadoop path which will be to used to run.')
    group_call.add_argument('--hive_cmd_env', default='/usr/local/apache-hive-1.2.0/bin/hive',
                            help='The hive path which will be to used to run.')
    group_call.add_argument('--hive_conf_path',
                            default=os.path.join(os.path.dirname(__file__), 'conf', 'hive_table_schema_and_data.yaml'),
                            help='The conf path which will be to used to update table.')
    group_call.add_argument('--hive_store_folder_path',  default='/user/hive/warehouse/external_table_data/',
                            help='The hadoop path which will be to used to run.')
    args = parser.parse_args()
    set_logging(args.log_folder_path, args.log_level)
    logger = logging.getLogger()
    logger.info('Begin update')
    try:
        process(args.hadoop_cmd_env, args.hive_cmd_env, args.hive_conf_path, args.hive_store_folder_path)
    except Exception, e:
        logger.error(e.message)
        raise
    logger.info('Update end')


if __name__ == '__main__':
    main()
