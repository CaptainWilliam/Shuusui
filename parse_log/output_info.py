#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import codecs
import sys
import os

__author__ = 'LH Liu'

reload(sys)
sys.setdefaultencoding('utf-8')


def write_into_file_for_page_and_track(final_info_lines, output_info, des_path_list):
    output_filename_for_page, output_filename_for_track = des_path_list[1].strip().split('and_')
    # page_output_path
    page_output_path = os.path.join(des_path_list[0], output_filename_for_page + des_path_list[2] + des_path_list[3])
    tmp_output_line_for_page = {}
    with codecs.open(page_output_path, mode='a', encoding='utf-8') as writer_final_info_for_page:
        for final_info_line_for_page in final_info_lines:
            if ((str(final_info_line_for_page.get('type')) == 'page') and
                    (str(final_info_line_for_page.get('visitor_user_id')) != '') and
                    (str(final_info_line_for_page.get('session_session_id')) != '') and
                    (str(final_info_line_for_page.get('page_custom_data_search_id')) != '')):
                tmp_output_line_for_page = {}
                for item in output_info:
                    tmp_output_line_for_page.setdefault(item, str(final_info_line_for_page.get(item, '')))
                writer_final_info_for_page.write('\t'.join(tmp_output_line_for_page.values()) + '\n')
    # track_output_path
    track_output_path = os.path.join(des_path_list[0], output_filename_for_track + des_path_list[2] + des_path_list[3])
    tmp_output_line_for_track = {}
    with codecs.open(track_output_path, mode='a', encoding='utf-8') as writer_final_info_for_track:
        for final_info_line_for_track in final_info_lines:
            if ((str(final_info_line_for_track.get('type')) == 'track') and
                    (str(final_info_line_for_track.get('visitor_user_id')) != '') and
                    (str(final_info_line_for_track.get('session_session_id')) != '')and
                    (str(final_info_line_for_track.get('page_custom_data_search_id')) != '')):
                tmp_output_line_for_track = {}
                for item in output_info:
                    tmp_output_line_for_track.setdefault(item, str(final_info_line_for_track.get(item, '')))
                writer_final_info_for_track.write('\t'.join(tmp_output_line_for_track.values()) + '\n')
    # get schema path
    des_path_list[-2] = 'schema'
    page_schema_path = os.path.join(des_path_list[0], output_filename_for_page + des_path_list[2] + des_path_list[3])
    with codecs.open(page_schema_path, mode='w', encoding='utf-8') as writer_final_schema_for_page:
        page_schema = ':string\n'.join(tmp_output_line_for_page.keys()) + ':string'
        writer_final_schema_for_page.write(page_schema)
    track_schema_path = os.path.join(des_path_list[0], output_filename_for_track + des_path_list[2] + des_path_list[3])
    # magic, do not question
    with codecs.open(track_schema_path, mode='w', encoding='utf-8') as writer_final_schema_for_track:
        if tmp_output_line_for_track.keys() == tmp_output_line_for_page.keys():
            track_schema = ':string\n'.join(tmp_output_line_for_track.keys()) + ':string'
        else:
            track_schema = ':string\n'.join(tmp_output_line_for_page.keys()) + ':string'
        writer_final_schema_for_track.write(track_schema)
    return [str(page_output_path), str(track_output_path), str(page_schema_path), str(track_schema_path)]


def write_into_file(final_info_lines, output_info, des_path_list):
    tmp_output_line = {}
    with codecs.open(os.path.join(des_path_list[0], str(des_path_list[1] + des_path_list[2] + des_path_list[3])),
                     mode='a', encoding='utf-8'
                     ) as writer_final_info:
        for final_info_line in final_info_lines:
            tmp_output_line = {}
            for single_item in output_info:
                tmp_output_line.setdefault(single_item, str(final_info_line.get(single_item, 'X')))
            writer_final_info.write('\t'.join(tmp_output_line.values()) + '\n')
    # get schema path
    des_path_list[-2] = 'schema'
    with codecs.open(os.path.join(des_path_list[0], des_path_list[1] + des_path_list[2] + des_path_list[3]),
                     mode='w', encoding='utf-8'
                     ) as writer_final_schema:
        writer_final_schema.write(':string\n'.join(tmp_output_line.keys())+':string')
