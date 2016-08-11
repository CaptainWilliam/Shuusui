#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from utils.hdfs_helper import HDFSHelper
import commands
import logging
import os

__author__ = 'LH Liu'

logger = logging.getLogger()


def find_all_the_datas_in_hdfs(table_data_path):
    if '${' in table_data_path.strip().split('/')[-1]:
        table_data_folder_without_time = '/'.join(table_data_path.strip().split('${', 1)[0].split('/')[:-1])
        file_name_without_time = table_data_path.strip().split('${')[0].split('/')[-1]
        # get all the file/folder in this path
        status, output = commands.getstatusoutput(
            "{} fs -ls {} ".format(HDFSHelper.hadoop_cmd_env, table_data_folder_without_time)
        )
        if status:
            raise IOError(output)
        folders_or_files_with_time = []
        for line in str(output).split('\n'):
            # filter other files
            if not (('Warning:' in line) or (line is '') or ('Found' in line) or ('available' in line)):
                if (file_name_without_time is not '') and (file_name_without_time in line):
                    deeper_folder = line.strip().split('/')[-1]
                    folders_or_files_with_time.append(deeper_folder)
        # get all date
        all_folders_or_files = {data_name: os.path.join(table_data_folder_without_time, data_name)
                                for data_name in folders_or_files_with_time}
        # return path, and the filename in hive(used to check if the table in hive is the newest)
        return all_folders_or_files
    elif '${' in '/'.join(table_data_path.strip().split('/')[:-1]):
        table_data_folder_without_time = '/'.join(table_data_path.strip().split('$')[0].split('/')[:-1])
        status, output = commands.getstatusoutput(
            "{} fs -ls {}".format(HDFSHelper.hadoop_cmd_env, table_data_folder_without_time)
        )
        if status:
            raise IOError(output)
        folders_with_time = []
        for line in str(output).split('\n'):
            if not (('Warning:' in line) or (line is '') or ('Found' in line) or ('available' in line)):
                deeper_folder = line.strip().split('/')[-1]
                folders_with_time.append(deeper_folder)
        backward_part = table_data_path.strip().split('$')[-1].split('/', 1)[-1]
        all_folders = {'{}_{}'.format(folder_name, table_data_path.strip().split('/')[-1]):
                           os.path.join(table_data_folder_without_time, folder_name, backward_part)
                       for folder_name in folders_with_time
                       }
        return all_folders
    else:
        try:
            raise ValueError
        except ValueError, e:
            logger.error(e.message + 'wrong format in data path')


def find_the_newest_data_in_hdfs(table_data_path):
    hive_file_name_and_all_data_path = find_all_the_datas_in_hdfs(table_data_path)
    # find th newest
    newest_date_name = reduce(lambda date_1, date_2: date_1 if date_1 > date_2 else date_2,
                              hive_file_name_and_all_data_path.keys()
                              )
    newest_data_path = hive_file_name_and_all_data_path.get(newest_date_name)
    return newest_date_name, newest_data_path


def update(hadoop_cmd_env, file_path_in_hdfs, table_path_in_hive, filename):
    # tmp_merge_location = os.path.join(os.path.dirname(__file__), 'test/data/tmp_merge_location', filename)
    tmp_merge_location = '/data0/weiboyi/azkaban/HdfsToHive/temp_merge_position'
    try:
        status = commands.getstatusoutput('{} fs -test -d {}'.format(hadoop_cmd_env, file_path_in_hdfs))[0]
        # path is folder
        if status == 0:
            if commands.getstatusoutput('test -e {}'.format(os.path.join(tmp_merge_location, filename)))[0]:
                status, output = commands.getstatusoutput(
                    '{} fs -getmerge {} {}'.format(hadoop_cmd_env, file_path_in_hdfs, tmp_merge_location)
                )
                if status:
                    raise IOError(output)
            status, message = commands.getstatusoutput(
                '{} fs -copyFromLocal {} {}/'.format(hadoop_cmd_env, os.path.join(tmp_merge_location, filename),
                                                     table_path_in_hive
                                                     )
            )
            if status:
                raise IOError(message)
            status, message = commands.getstatusoutput(
                'rm {}'.format(os.path.join(tmp_merge_location, filename))
            )
            if status:
                raise
        # path is file just insert and overwrite
        else:
            status, message = commands.getstatusoutput(
                '{} fs -cp {} {}'.format(hadoop_cmd_env, file_path_in_hdfs, os.path.join(table_path_in_hive, filename))
            )
            if status:
                raise IOError(message)
    except Exception, e:
        logger.error(e.message)


def update_table(hadoop_cmd_env, hive_store_folder_path, table_name, table_info_detail):
    # init HDFSHelper.hadoop_cmd_env
    HDFSHelper.hadoop_cmd_env = hadoop_cmd_env
    # process by total
    if table_info_detail.get('update_mode') == 'total':
        # get the newest file in this path, and return the path with the newest file name
        table_data_folder = table_info_detail.get('data')
        newest_filename, file_path = find_the_newest_data_in_hdfs(table_data_folder)
        # check file or dir
        table_path_in_hive = os.path.join(hive_store_folder_path, table_name)
        try:
            HDFSHelper.check_hdfs_path(os.path.join(table_path_in_hive, newest_filename))
        except Exception, e:
            logger.info(e.message)
            try:
                commands.getstatusoutput('{} fs -rmr {}/*'.format(hadoop_cmd_env, table_path_in_hive))
            except Exception, e:
                logger.info(e.message)
            # update total
            update(hadoop_cmd_env, file_path, table_path_in_hive, newest_filename)
    # process by incremental
    else:
        # get the newest file in this path, and return the path with the newest file name
        table_data_folder = table_info_detail.get('data')
        hive_file_name_and_all_data_path = find_all_the_datas_in_hdfs(table_data_folder)
        for filename, file_path in hive_file_name_and_all_data_path.items():
            table_path_in_hive = os.path.join(hive_store_folder_path, table_name)
            try:
                HDFSHelper.check_hdfs_path(os.path.join(table_path_in_hive, filename))
            except IOError, e:
                logger.info(e.message)
                update(hadoop_cmd_env, file_path, table_path_in_hive, filename)
