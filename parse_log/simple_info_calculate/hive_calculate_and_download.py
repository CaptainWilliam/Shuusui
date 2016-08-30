#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import commands
import datetime
import logging

__author__ = 'LH Liu'

logger = logging.getLogger()


def download_file_from_hive(hive_cmd_env, download_hql, hql_date_diff, data_store_path, update_mode):
    # calculate date diff and then add it to hql
    if hql_date_diff:
        hql_date = str((datetime.datetime.today() + datetime.timedelta(hql_date_diff)).strftime("%Y-%m-%d"))
        download_hql = download_hql.format(hql_date)
    # update totally
    if update_mode == 'total':
        hql_cmd = '{} -S -e "{}" > {}'.format(hive_cmd_env, download_hql, data_store_path)
        status = commands.getstatusoutput(hql_cmd)[0]
    # update incrementally(with time)
    else:
        today_date = datetime.datetime.today().strftime("_%Y_%m_%d")
        data_store_path_list = data_store_path.strip().split('.')
        data_store_path_list[-2] += today_date
        data_store_path_with_time = '.'.join(data_store_path_list)
        hql_cmd = '{} -S -e "{}" > {}'.format(hive_cmd_env, download_hql, data_store_path_with_time)
        status = commands.getstatusoutput(hql_cmd)[0]
    if status:
        try:
            raise IOError
        except IOError:
            logger.info(IOError.message)
