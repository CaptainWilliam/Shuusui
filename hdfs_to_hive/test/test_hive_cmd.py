#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from hdfs_to_hive.update_table_in_hive import find_all_the_datas_in_hdfs
from hdfs_to_hive.update_table_in_hive import find_the_newest_data_in_hdfs
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
        ori_path = 'E:/PythonProject/Work/work_1/hdfs_to_hive/test/data/tmp_merge_location/.sohu_self_media_video_account_report_week_v1.0_${time}.crc'
        result = ['E:/PythonProject/Work/work_1/hdfs_to_hive/test/data/tmp_merge_location/.sohu_self_media_video_account_report_week_v1.0_201631.crc']
        self._list_equal(find_all_the_datas_in_hdfs(ori_path), result)

    def test_find_the_newest_data_in_hdfs(self):
        ori_list =  'E:/PythonProject/Work/work_1/hdfs_to_hive/test/data/tmp_merge_location/.sohu_self_media_video_account_report_week_v1.0_${time}.crc'
        result = 'E:/PythonProject/Work/work_1/hdfs_to_hive/test/data/tmp_merge_location/.sohu_self_media_video_account_report_week_v1.0_201631.crc'
        self.assertEquals(find_the_newest_data_in_hdfs(ori_list), result)


if __name__ == '__main__':
    unittest.main()
