# coding=utf-8
from __future__ import unicode_literals
import codecs
import os


class SchemaHelper(object):
    """
    定义一个工具类，提供schema文件的相关操作
    """
    @staticmethod
    def _load_schema_file(schema_file_path):
        if not os.path.isfile(schema_file_path):
            raise IOError('Schema file {} not existed.'.format(schema_file_path))
        schema_list = []
        with codecs.open(schema_file_path, mode='r', encoding='utf-8') as schema_file:
            for line in schema_file:
                field_list = line.strip(', \r\n').split(':')
                field_name = field_list[0].strip()
                field_type = field_list[1].strip()
                schema_list.append((field_name, field_type))
        return schema_list

    @staticmethod
    def _check_schema_file(schema_list, input_schema_file_path):
        if not isinstance(schema_list, list):
            raise TypeError('The schema_list be a list.')
        input_schema_list = SchemaHelper._load_schema_file(input_schema_file_path)
        for schema_item in schema_list:
            if schema_item not in input_schema_list:
                return False
        return True

    @staticmethod
    def get_schema_index_dict(required_schema_list, input_schema_file_path):
        if SchemaHelper._check_schema_file(required_schema_list, input_schema_file_path):
            input_schema_list = SchemaHelper._load_schema_file(input_schema_file_path)
            schema_name_index_dict = {}
            for index, input_schema in enumerate(input_schema_list):
                if input_schema in required_schema_list:
                    schema_name_index_dict[input_schema[0]] = index
            return schema_name_index_dict
        else:
            raise IOError('The input schema file is invalid, please check if contain all the required fields.')

    @staticmethod
    def output_schema_to_file(schema_list, outfile_path):
        if not isinstance(schema_list, list):
            raise TypeError('The schema_list be a list.')
        schema_string_list = []
        for schema_name, schema_type in schema_list:
            schema_string_list.append('{}:{}'.format(schema_name, schema_type))
        with codecs.open(outfile_path, mode='w', encoding='utf-8') as outfile:
            outfile.write(',\n'.join(schema_string_list) + '\n')
