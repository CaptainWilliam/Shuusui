#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import commands
import datetime

__author__ = 'LH Liu'


def download_file_from_hive(hive_cmd_env, download_hql, data_store_path):
    date_str = str(datetime.datetime.today().strftime("_%Y_%m_%d"))
    data_store_path_list = data_store_path.strip().split('.')
    data_store_path_list[-2] += date_str
    final_data_store_path = '.'.join(data_store_path_list)
    status = commands.getstatusoutput(
        '{} -S -e "{}" > {}'.format(hive_cmd_env, download_hql, final_data_store_path)
    )[0]
    if status:
        raise IOError
    commands.getstatusoutput('rm {}'.format(data_store_path))
    status = commands.getstatusoutput('cp {} {}'.format(final_data_store_path, data_store_path))[0]
    if status:
        raise
