#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import codecs
import os

__author__ = 'LH Liu'


def find_all_files(ori_path, log_file_regular_expression):
    file_path_dict = {}
    for root, sub_folders, files in os.walk(ori_path):
        for ordinary_file in files:
            is_log_file = log_file_regular_expression.match(ordinary_file)
            if is_log_file:
                full_path = os.path.join(ori_path, ordinary_file)
                file_path_dict.setdefault(full_path, is_log_file.group(1) + is_log_file.group(2) + is_log_file.group(3))
    # return all the files and its date
    return file_path_dict


def first_parse_log(ori_path, log_parse_method):
    with codecs.open(ori_path, mode='r', encoding='utf-8') as log_reader:
        for line in log_reader:
            line_split_to_lines = [line]
            line_info = {}
            for item_key, item_parse_method in log_parse_method.items():
                item_value = ''
                for splited_line in line_split_to_lines:
                    is_item = item_parse_method.match(splited_line)
                    if is_item:
                        item_value = is_item.group(1)
                        line_info.setdefault(item_key, item_value)
                        # split the line into 2 parts by the matched item the put them into the list
                        split_line_forward_part = splited_line.split(item_value, 1)[0]
                        split_line_backward_part = splited_line.split(item_value, 1)[1]
                        line_split_to_lines.insert(line_split_to_lines.index(splited_line),
                                                   split_line_backward_part)
                        line_split_to_lines[line_split_to_lines.index(splited_line)] = split_line_forward_part
                        break
                line_info.setdefault(item_key, item_value)
            # return the line_info
            yield line_info


def first_parse_log_for_search_result(ori_path, log_parse_method):
    with codecs.open(ori_path, mode='r', encoding='utf-8') as log_reader:
        for line in log_reader:
            line_info = {}
            json_info_search_result_separator = log_parse_method.get('json_info_search_result')
            if (json_info_search_result_separator in line) and \
                    ('rows' in line) and ('total' in line) and ('search_id' in line):
                line_info.setdefault('json_info', line.strip('\n').split(json_info_search_result_separator)[1])
                yield line_info
