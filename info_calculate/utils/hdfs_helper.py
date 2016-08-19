# coding=utf-8
from __future__ import unicode_literals
import os
import commands


class HDFSHelper(object):
    hadoop_cmd_env = '/usr/local/hadoop/bin/hadoop'

    @staticmethod
    def _check_hdfs_source(hdfs_source_path, source_type):
        if commands.getstatusoutput('{} fs -test -e {}'.format(HDFSHelper.hadoop_cmd_env, hdfs_source_path))[0]:
            raise IOError('HDFS source {} not existed.'.format(hdfs_source_path))
        status = commands.getstatusoutput('{} fs -test -d {}'.format(HDFSHelper.hadoop_cmd_env, hdfs_source_path))[0]
        if status == 0:
            found_type = 'folder'
        else:
            found_type = 'file'
        if found_type != source_type:
            raise TypeError('HDFS source {} is not type {}.'.format(hdfs_source_path, source_type))
        return True

    @staticmethod
    def download_hdfs_source_to_local(hdfs_source_path, local_path, source_type='file'):
        """
        :param hdfs_source_path: can be a folder path or file path
        :param local_path: can be a folder path or file path
        :param source_type: must be file of folder
        :param download_force_mode: if local file existed, will be replace or not
        :return:
        """
        if HDFSHelper._check_hdfs_source(hdfs_source_path, source_type):
            if source_type == 'file':
                status, message = commands.getstatusoutput('{} fs -get {} {}'.format(HDFSHelper.hadoop_cmd_env, hdfs_source_path, local_path))
            else:
                status, message = commands.getstatusoutput('{} fs -getmerge {} {}'.format(HDFSHelper.hadoop_cmd_env, hdfs_source_path, local_path))
                # remove the crc verification files
                os.system('rm {}/.*.crc'.format(local_path))
            if status:
                raise IOError(message)

    @staticmethod
    def upload_local_file_to_hdfs(local_file_path, hdfs_file_path, upload_force_mode=False):
        if commands.getstatusoutput('{} fs -test -e {}'.format(HDFSHelper.hadoop_cmd_env, hdfs_file_path))[0]:
            status, message = commands.getstatusoutput('{} fs -put {} {}'.format(HDFSHelper.hadoop_cmd_env, local_file_path, hdfs_file_path))
            if status:
                raise IOError(message)
        else:
            if upload_force_mode:
                os.system('{} fs -rm {}'.format(HDFSHelper.hadoop_cmd_env, hdfs_file_path))
                status, message = commands.getstatusoutput('{} fs -put {} {}'.format(HDFSHelper.hadoop_cmd_env, local_file_path, hdfs_file_path))
                if status:
                    raise IOError(message)

    @staticmethod
    def make_file_in_hdfs(file_name, hdfs_folder_path):
        if commands.getstatusoutput(
                '{} fs -test -e {}'.format(HDFSHelper.hadoop_cmd_env, os.path.join(hdfs_folder_path, file_name)))[0]:
            status, message = commands.getstatusoutput(
                '{} fs -touchz {}/{}'.format(HDFSHelper.hadoop_cmd_env, hdfs_folder_path, file_name))
            if status:
                raise IOError(message)

    @staticmethod
    def make_dir_in_hdfs(hdfs_folder_path):
        if commands.getstatusoutput('{} fs -test -e {}'.format(HDFSHelper.hadoop_cmd_env, hdfs_folder_path))[0]:
            status, message = commands.getstatusoutput(
                '{} fs -mkdir {}'.format(HDFSHelper.hadoop_cmd_env, hdfs_folder_path))
            if status:
                raise IOError(message)

    # add some more funcs
    @staticmethod
    def check_hdfs_path(hdfs_source_path):
        if commands.getstatusoutput('{} fs -test -e {}'.format(HDFSHelper.hadoop_cmd_env, hdfs_source_path))[0]:
            raise IOError('HDFS source {} not existed.'.format(hdfs_source_path))

    @staticmethod
    def read_the_only_file_from_the_path(folder_or_file_path):
        HDFSHelper.check_hdfs_path(folder_or_file_path)
        status = commands.getstatusoutput(
            '{} fs -test -d {}'.format(HDFSHelper.hadoop_cmd_env, folder_or_file_path)
        )[0]
        if status == 0:
            # status_ls, output_ls = commands.getstatusoutput(
            #    "{} fs -ls {}".format(HDFSHelper.hadoop_cmd_env, folder_or_file_path)
            # )
            return folder_or_file_path + '/*'
        else:
            return folder_or_file_path
