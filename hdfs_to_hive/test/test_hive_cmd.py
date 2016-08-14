#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from update_table_in_hive import find_all_the_datas_in_hdfs
from update_table_in_hive import find_the_newest_data_in_hdfs
import unittest

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

    def test_find_all_the_datas_in_hdfs(self):
        ori_path = '/XXX/XXX_${time}.crc'
        result = ['/XXX/XXX_201631.crc']
        self._list_equal(find_all_the_datas_in_hdfs(ori_path), result)

    def test_find_the_newest_data_in_hdfs(self):
        ori_list = '/XXX/XXX_${time}.crc'
        result = '/XXX/XXX_${time}.crc'
        self.assertEquals(find_the_newest_data_in_hdfs(ori_list), result)


if __name__ == '__main__':
    unittest.main()
