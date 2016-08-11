#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import codecs
import yaml

__author__ = 'LH Liu'


def load_conf(conf_path):
    with codecs.open(conf_path, mode='r', encoding='utf-8') as conf_reader:
        conf_info = yaml.load(conf_reader)
        # use to judge if this file is a log file
        log_file = conf_info.get('log_file')
        # the methods to parse the log
        first_log_parse_method = conf_info.get('first_log_parse_method')
        second_log_parse_method = conf_info.get('second_log_parse_method')
        # the  method to parse the output info and output them
        output_info = conf_info.get('output_info')
        return log_file, first_log_parse_method, second_log_parse_method, output_info
