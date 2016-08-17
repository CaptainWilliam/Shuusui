#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from first_parse_log import find_all_files
from first_parse_log import first_parse_log
import unittest
import re
import os

__author__ = 'LH Liu'


class TestParseLog(unittest.TestCase):
    def _list_equal(self, list_one, list_another):
        set_one = set(list_one)
        set_another = set(list_another)
        msg = '{} != {}'.format(list_one, list_another)
        if len(set_one) != len(set_another):
            raise self.failureException(msg)
        for element in set_one:
            if element not in set_another:
                raise self.failureException(msg)

    def test_find_all_files(self):
        ori_path = os.path.join(os.path.dirname(__file__), 'data/input')
        log_file_regular_expression = re.compile(r'web\.analytics\.access_(\d{6})(\d{2})\d{4}\.log')
        result = {
            'E:/PythonProject/Work/work_2/alter_code/data/input\\web.analytics.access_201606250000.log': '20160625'
        }
        self._list_equal(find_all_files(ori_path, log_file_regular_expression), result)
        ori_path = os.path.join(os.path.dirname(__file__), 'data/output')
        log_file_regular_expression = re.compile(r'web\.analytics\.access_(\d{6})(\d{2})\d{4}\.log')
        result = {}
        self._list_equal(find_all_files(ori_path, log_file_regular_expression), result)

    def test_parse_log(self):
        ori_path = os.path.join(os.path.dirname(__file__), 'data/input', 'web.analytics.access_201606250000.log')
        log_parse_method = {'client_ip': re.compile(r'^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s\-\s\-\s\[')}
        result = {'client_ip': '183.69.225.138'}
        self.assertEquals(first_parse_log(ori_path, log_parse_method).next(), result)


if __name__ == '__main__':
    unittest.main()
