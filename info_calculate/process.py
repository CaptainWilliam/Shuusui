#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from utils.config_helper import ConfigHelper
from hive_calculate_and_download import download_file_from_hive
from wrap_to_json_and_upload_to_es import wrap_to_json
from wrap_to_json_and_upload_to_es import upload_to_es
import os

__author__ = 'LH Liu'


def process(hive_cmd_env):
    # read conf 1: download from hive
    hive_calculate_and_download = ConfigHelper.load_config(
        os.path.join(os.path.dirname(__file__), 'conf', 'hive_calculate_and_download.yaml')
    )
    for hql_number, hql_detail in hive_calculate_and_download.items():
        # download files from hive after calculate
        download_file_from_hive(
            hive_cmd_env, hql_detail.get('hql'), hql_detail.get('data_store_path')
        )

    # read conf 2: wrap to json and upload
    wrap_to_json_and_upload_to_es = ConfigHelper.load_config(
        os.path.join(os.path.dirname(__file__), 'conf', 'wrap_to_json_and_upload_to_es.yaml')
    )
    for file_number, process_detail in wrap_to_json_and_upload_to_es.items():
        # wrap the downloaded files to json
        json_schema_path, json_file_path = wrap_to_json(
            process_detail.get('schema_path'), process_detail.get('data_path'),
            process_detail.get('index_name'), process_detail.get('type_name')
        )

        # and then upload them to it
        upload_to_es(process_detail.get('index_name'), json_schema_path, json_file_path)
