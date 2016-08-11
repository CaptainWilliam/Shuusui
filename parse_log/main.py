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
    parser = argparse.ArgumentParser(prog='Log file parser')
    log_parameter = parser.add_argument_group('Log parameters', 'Parameters are used for logging')
    log_parameter.add_argument('--log_folder_path', default='logs', help='The log output position.')
    log_parameter.add_argument('--log_level', default='DEBUG',
                               choices=['DEBUG', 'INFO', 'ERROR', 'WARNING', 'CRITICAL'], help='The log level.')
    process_parameter = parser.add_argument_group('Process parameters', 'Parameters are used for main process')
    # /data / web_analytics_log / 2016
    process_parameter.add_argument('--input_folder_path', help='The folder contains log files.')
    # /data0/weiboyi/azkaban/MSE/log/log_parse_to_info/2016
    process_parameter.add_argument('--output_folder_path', help='The folder stores the results.')
    process_parameter.add_argument('--output_file_name', default='pages_and_tracks_', help='The file name.')
    process_parameter.add_argument('--output_file_type', default='.tsv', help='The file type.')
    args = parser.parse_args()
    set_logging(args.log_folder_path, args.log_level)
    logger = logging.getLogger()
    logger.info('Begin the process')
    try:
        process(args.input_folder_path, args.output_folder_path,
                args.output_file_name, args.output_file_type)
    except Exception as e:
        logger.error(e)
        raise
    logger.info('Process end')


if __name__ == '__main__':
    main()
