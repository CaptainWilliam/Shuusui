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
    # set log file path
    log_config = ConfigHelper.load_config(os.path.join(os.path.dirname(__file__), 'conf', 'log.yaml'))
    log_file_name = 'log'
    if not os.path.exists(log_folder_path):
        os.makedirs(log_folder_path)
    log_file_path = os.path.join(log_folder_path, log_file_name)
    log_config['handlers']['file_handler']['filename'] = log_file_path
    log_config['handlers']['file_handler']['level'] = log_level
    logging.config.dictConfig(log_config)


def main():
    # read parameters
    parser = argparse.ArgumentParser(prog='Dake recommendation parser')
    log_parameter = parser.add_argument_group('Log parameters', 'Parameters are used for logging.')
    log_parameter.add_argument('--log_folder_path', default='logs', help='The log output position.')
    log_parameter.add_argument('--log_level', default='DEBUG',
                               choices=['DEBUG', 'INFO', 'ERROR', 'WARNING', 'CRITICAL'], help='The log level.')
    group_call = parser.add_argument_group('Group call', 'Parameters are used for calling this plugin.')
    group_call.add_argument('--hive_cmd_env', default='XXX/hive',
                            help='The hive path which will be to used to run.')
    args = parser.parse_args()
    set_logging(args.log_folder_path, args.log_level)
    logger = logging.getLogger()
    logger.info('Begin program')
    try:
        process(args.hive_cmd_env)
    except Exception, e:
        logger.error(e.message)
        raise
    logger.info('Program end')


if __name__ == '__main__':
    main()
