#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import codecs

__author__ = 'LH Liu'


def write_into_file(final_info_lines, output_info, des_path):
    with codecs.open(des_path, mode='a', encoding='utf-8') as writer_final_info:
        for final_info_line in final_info_lines:
            tmp_output_line = []
            for single_item in output_info:
                tmp_output_line.append(final_info_line.get(single_item, ''))
            final_output_line = map(str, tmp_output_line)
            writer_final_info.write('\t'.join(final_output_line) + "\n")
