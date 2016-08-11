#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from load_conf import load_conf
from first_parse_log import find_all_files
from first_parse_log import first_parse_log
from second_parse_log import load_json_info
from second_parse_log import get_json_info_details
from second_parse_log import second_parse_log
from output_info import write_into_file
import re
import os

__author__ = 'LH Liu'


def process(input_folder_path=None, output_path=None, output_file_name=None, output_file_type=None):
    # load conf
    log_file, first_log_parse_method, second_log_parse_method, output_info = load_conf(
        os.path.join(os.path.dirname(__file__), 'conf', 'item.yaml')
    )
    log_file_regular_expression = re.compile(eval(log_file.get('regular_expression')))
    # basically all the methods are regular expressions
    first_log_parse_method_re_dict = {
        item_name: re.compile(eval(item_re)) for item_name, item_re in first_log_parse_method.items()
        }
    second_parse_explicit_info = second_log_parse_method.get('explicit')
    second_parse_implicit_info = second_log_parse_method.get('implicit')
    second_parse_special_info = second_log_parse_method.get('special')

    # find all the files from the folder path
    tmp_log_file_path = find_all_files(input_folder_path, log_file_regular_expression)
    log_file_path = {
        ori_file_path: os.path.join(output_path, output_file_name + output_file_date + output_file_type)
        for ori_file_path, output_file_date in tmp_log_file_path.items()
        }

    # parse log from all the files
    for ori_file_path, output_file_date in log_file_path.items():
        # first parse log
        raw_data_list = first_parse_log(ori_file_path, first_log_parse_method_re_dict)

        # second parse log
        second_parsed_data = []
        for line_dict in raw_data_list:
            tmp_json_dict = load_json_info(line_dict.get('json_info'))
            # parse json
            json_dict_keys, json_dict_values = get_json_info_details(tmp_json_dict, [], [], [])
            json_dict = dict(zip(json_dict_keys, json_dict_values))
            # log info extend specific json info
            tmp_data_dict = dict(line_dict, **json_dict)
            # get output line from the tmp data dict
            second_parsed_line = second_parse_log(tmp_data_dict, second_parse_explicit_info,
                                                  second_parse_implicit_info, second_parse_special_info)
            second_parsed_data.append(second_parsed_line)

        # write into files
        write_into_file(second_parsed_data, output_info, output_file_date)
