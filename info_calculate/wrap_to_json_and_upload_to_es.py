#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import commands
import codecs
import json
import os

__author__ = 'LH Liu'


def read_schema(schema_path):
    schema_cols = []
    schema_col_types = []
    with codecs.open(schema_path, mode='r', encoding='utf-8') as schema_reader:
        for line in schema_reader:
            if line.strip() != '':
                if ':' in line:
                    schema_col = line.strip().split(':')[0]
                    schema_col_type = line.strip().split(':')[1]
                    schema_cols.append(schema_col)
                    schema_col_types.append(schema_col_type)
                else:
                    schema_col = line.strip().split(' ')[0]
                    schema_col_type = line.strip().split(' ')[1]
                    schema_cols.append(schema_col)
                    schema_col_types.append(schema_col_type)
    # if return dict(zip(schema_cols, schema_col_types)), then when u use dict.keys(), the order will be disrupted
    return schema_cols, schema_col_types


def wrap_to_json(schema_path, data_path, index_name, type_name):
    # read schemas
    schema_col_list, schema_col_types_list = read_schema(schema_path)
    schema_col_dict = dict(zip(schema_col_list, schema_col_types_list))
    # zip schemas and datas and dump to json
    schema_and_data_lines = []
    with codecs.open(data_path, mode='r', encoding='utf-8') as data_reader:
        for line in data_reader:
            if line.strip() != '':
                data_col_list = line.strip().split('\t')
                if len(schema_col_list) is not len(data_col_list):
                    raise KeyError
                schema_and_data_dict = dict(zip(schema_col_list, data_col_list))
                schema_and_data_lines.append(schema_and_data_dict)
    # get index detail
    json_schema_store_path = os.path.join('/'.join(schema_path.split('/')[:-1]), index_name + '.json')
    index_properties = {col_name: {"type": col_type, "index": "not_analyzed"}
                        for col_name, col_type in schema_col_dict.items()
                        }
    index_json = {"mappings": {"log": dict(properties=index_properties)}}
    # get json files(only use to create index)
    with codecs.open(json_schema_store_path, mode='w', encoding='utf-8') as index_json_writer:
        index_json_writer.write(json.dumps(index_json) + '\n')
    json_file_store_path = '.'.join(data_path.split('.')[:-1]) + '.json'
    title_dict = {"index": {"_index": index_name, "_type": type_name}}
    # get json files(only use to keep the newest data)
    with codecs.open(json_file_store_path, mode='w', encoding='utf-8') as json_writer:
        for schema_and_data_dict in schema_and_data_lines:
            json_writer.write(json.dumps(title_dict) + '\n')
            json_writer.write(json.dumps(schema_and_data_dict) + '\n')
    # return the json file store path
    return json_schema_store_path, json_file_store_path


def upload_to_es(index_name, json_schema_path, json_file_path):
    # check is index exist
    status, output = commands.getstatusoutput(
        'curl -XHEAD -i \'http://XXX:9200/{}\''.format(index_name)
    )
    print output
    if '404' in output:
        # create table first
        status = commands.getstatusoutput(
            'curl -XPOST http://XXX:9200/{} --data-binary @{}'.format(index_name, json_schema_path)
        )[0]
        if status:
            raise IOError
    status = commands.getstatusoutput(
        'curl -XPOST http://XXX:9200/_bulk --data-binary @{}'.format(json_file_path)
    )[0]
    if status:
        raise IOError
