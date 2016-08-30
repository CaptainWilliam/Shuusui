# coding=utf-8
from __future__ import unicode_literals
from utils.hdfs_helper import HDFSHelper
import commands


class HDFSProtocolHelper(object):
    @staticmethod
    def _get_source_data_folder(hdfs_source_general_path):
        return '/'.join(hdfs_source_general_path.split('/')[:-1])

    @staticmethod
    def _get_source_schema_folder(hdfs_source_general_path):
        return '/'.join(hdfs_source_general_path.split('/')[:-2]) + '/schema'

    @staticmethod
    def _get_source_data_base_name(hdfs_source_general_path):
        return hdfs_source_general_path.split('/')[-1]

    @staticmethod
    def _get_source_data_schema_version(hdfs_source_path):
        base_name = HDFSProtocolHelper._get_source_data_base_name('_'.join(hdfs_source_path.split('_')[:-2]))
        full_name = hdfs_source_path.split('/')[-1]
        return full_name[len(base_name) + 1:full_name.rfind('_')]

    @staticmethod
    def get_hdfs_latest_n_available_date(hdfs_source_general_path, item_count=1):
        hdfs_source_path_folder = HDFSProtocolHelper._get_source_data_folder(hdfs_source_general_path)
        status, date_array_string = commands.getstatusoutput('{} fs -ls {}/available_* | tail -n {}'.format(HDFSHelper.hadoop_cmd_env, hdfs_source_path_folder, item_count))
        if status:
            raise IOError(date_array_string)
        date_array = []
        for line in date_array_string.split('\n'):
            available_string = line.strip().split('/')[-1]
            if available_string and available_string.find('available') == 0:
                date_array.append(available_string.split('_')[-1])
        return date_array

    @staticmethod
    def get_hdfs_range_available_date(hdfs_source_general_path, start_date, end_date):
        if start_date > end_date:
            raise IOError('The start date {} is greater than end data {}'.format(start_date, end_date))
        hdfs_source_path_folder = HDFSProtocolHelper._get_source_data_folder(hdfs_source_general_path)
        status, date_array_string = commands.getstatusoutput('{} fs -ls {}/available_*'.format(HDFSHelper.hadoop_cmd_env, hdfs_source_path_folder))
        if status:
            raise IOError(date_array_string)
        date_array = []
        for line in date_array_string.split('\n'):
            available_string = line.strip().split('/')[-1]
            if available_string and available_string.find('available') == 0:
                current_date = available_string.split('_')[-1]
                if unicode(start_date) <= current_date <= unicode(end_date):
                    date_array.append(current_date)
        return date_array

    @staticmethod
    def _get_hdfs_source_path_and_schema_path_by_date_list(hdfs_source_general_path, date_array):
        hdfs_source_path_folder = HDFSProtocolHelper._get_source_data_folder(hdfs_source_general_path)
        source_path_array = []
        latest_schema_version = -1.0
        status, data_array_string = commands.getstatusoutput( '{} fs -ls {}'.format(HDFSHelper.hadoop_cmd_env, hdfs_source_path_folder))
        if status:
            raise IOError(data_array_string)
        for date_string in date_array:
            for line in data_array_string.split('\n'):
                source_path = line.strip().split(' ')[-1]
                schema_version = HDFSProtocolHelper._get_source_data_schema_version(source_path)
                if source_path == '{}_{}_{}'.format(hdfs_source_general_path, schema_version, date_string):
                    source_path_array.append(source_path)
                    if float(schema_version[1:]) > latest_schema_version:
                        latest_schema_version = float(schema_version[1:])
        schema_folder = HDFSProtocolHelper._get_source_schema_folder(hdfs_source_general_path)
        base_name = HDFSProtocolHelper._get_source_data_base_name(hdfs_source_general_path)
        hdfs_schema_path = '{}/schema_{}_v{}'.format(schema_folder, base_name, latest_schema_version)
        return source_path_array, hdfs_schema_path

    @staticmethod
    def get_hdfs_latest_n_available_source_path_and_schema_path(hdfs_source_general_path, item_count=1):
        date_array = HDFSProtocolHelper.get_hdfs_latest_n_available_date(hdfs_source_general_path, item_count)
        return HDFSProtocolHelper._get_hdfs_source_path_and_schema_path_by_date_list(hdfs_source_general_path, date_array)

    @staticmethod
    def get_hdfs_range_available_source_path_and_schema_path(hdfs_source_general_path, start_date, end_date):
        date_array = HDFSProtocolHelper.get_hdfs_range_available_date(hdfs_source_general_path, start_date, end_date)
        return HDFSProtocolHelper._get_hdfs_source_path_and_schema_path_by_date_list(hdfs_source_general_path, date_array)

    @staticmethod
    def download_hdfs_latest_n_source_to_local(hdfs_source_general_path, local_path, schema_local_path, item_count=1, source_type='file'):
        source_path_array, hdfs_schema_path = HDFSProtocolHelper.get_hdfs_latest_n_available_source_path_and_schema_path(hdfs_source_general_path, item_count)
        # download the data to local
        for source_path in source_path_array:
            HDFSHelper.download_hdfs_source_to_local(source_path, local_path, source_type)
        # download latest schema to local
        HDFSHelper.download_hdfs_source_to_local(hdfs_schema_path, schema_local_path)

    @staticmethod
    def download_hdfs_range_source_to_local(hdfs_source_general_path, local_path, schema_local_path, start_date, end_date, source_type='file'):
        source_path_array, hdfs_schema_path = HDFSProtocolHelper.get_hdfs_range_available_source_path_and_schema_path(hdfs_source_general_path, start_date, end_date)
        # download the data to local
        for source_path in source_path_array:
            HDFSHelper.download_hdfs_source_to_local(source_path, local_path, source_type)
        # download latest schema to local
        HDFSHelper.download_hdfs_source_to_local(hdfs_schema_path, schema_local_path)

    @staticmethod
    def upload_local_file_to_hdfs(hdfs_source_general_path, local_source_path, valid_time, local_schema_path, schema_version, upload_force_mode=False):
        # upload the data to hdfs
        hdfs_file_path = '{}_v{}_{}'.format(hdfs_source_general_path, schema_version, valid_time)
        HDFSHelper.upload_local_file_to_hdfs(local_source_path, hdfs_file_path, upload_force_mode)
        # upload the schema to hdfs
        schema_folder = HDFSProtocolHelper._get_source_schema_folder(hdfs_source_general_path)
        base_name = HDFSProtocolHelper._get_source_data_base_name(hdfs_source_general_path)
        hdfs_schema_path = '{}/schema_{}_v{}'.format(schema_folder, base_name, schema_version)
        HDFSHelper.upload_local_file_to_hdfs(local_schema_path, hdfs_schema_path, upload_force_mode)
        # upload the available file
        source_folder = HDFSProtocolHelper._get_source_data_folder(hdfs_source_general_path)
        available_file_name = 'available_{}'.format(valid_time)
        HDFSHelper.make_file_in_hdfs(available_file_name, source_folder)
