#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from utils.config_helper import ConfigHelper
from hive_calculate_and_download import download_file_from_hive
import os

__author__ = 'LH Liu'


def process(hive_cmd_env):
    # read conf 2: download from hive
    hive_calculate_and_download = ConfigHelper.load_config(
        os.path.join(os.path.dirname(__file__), 'conf', 'hive_calculate_and_download.yaml')
    )
    for hql_number, hql_detail in hive_calculate_and_download.items():
        # download files from hive after calculate
        download_file_from_hive(
            hive_cmd_env, hql_detail.get('hql'), hql_detail.get('hql_date_diff'),
            hql_detail.get('data_store_path'), hql_detail.get('update_mode')
        )
