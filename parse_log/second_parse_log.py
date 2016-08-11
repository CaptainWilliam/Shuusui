#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from urllib import unquote
import logging
import json
import re

__author__ = 'LH Liu'

logger = logging.getLogger()


def load_json_info(original_json_info):
    uncoded_json_info = unquote(original_json_info)
    json_info = {}
    try:
        json_info = json.loads(uncoded_json_info)
    except Exception as e:
        logger.error(e)
    return json_info


# iterate to get all the json info
def get_json_info_details(json_info, inconsecutive_name=None, keys_set=None, values_set=None):
    # is dict: iterate
    if isinstance(json_info, dict):
        if json_info == {}:
            final_name = "_".join(inconsecutive_name)
            keys_set.append(final_name)
            values_set.append("")
        else:
            for item_name, item_value in json_info.items():
                # PUSH iteration POP
                inconsecutive_name.append(item_name)
                keys_set, values_set = get_json_info_details(item_value, inconsecutive_name, keys_set, values_set)
                inconsecutive_name.pop()
    # is list: iterate
    elif isinstance(json_info, list):
        for item in json_info:
            get_json_info_details(item, inconsecutive_name, keys_set, values_set)
    # is final info
    else:
        final_name = "_".join(inconsecutive_name)
        keys_set.append(final_name)
        if json_info:
            values_set.append(json_info)
        else:
            values_set.append("")
    return keys_set, values_set


def second_parse_log(line_info, oie=None, oii=None, ois=None):
    # 1.explicit
    output_info_explicit = oie
    # (1.parsed value -> 2.default value -> 3.None(''))
    final_line = {
        item_name: (line_info.get(item_name) if line_info.get(item_name)
                    else (item_value.get('default') if item_value.get('default') else ''))
        for item_name, item_value in output_info_explicit.items()
    }
    # 2.implicit
    output_info_implicit = oii
    for item_name, item_value in output_info_implicit.items():
        re_list = [re.compile(eval(item_default_value))
                   for item_regular_expression, item_default_value in item_value.items()]
        tmp_value = ''
        for re_item in re_list:
            is_re_item = re_item.match(str(line_info.get('page_url')))
            if is_re_item:
                tmp_value = str(is_re_item.group(1))
                break
        final_line.setdefault(item_name, tmp_value)
    # 3.special
    output_info_special = ois
    filters_and_sorts = {}.fromkeys(output_info_special.get('possible_items').keys(), '')
    re_item = re.compile(eval(output_info_special.get('regular_expression')))
    all_filters_and_sorts = re_item.findall(str(line_info.get('page_url')))
    for filter_or_sort in all_filters_and_sorts:
        f_key = str(filter_or_sort.split('=')[0])
        f_value = str(filter_or_sort.split('=')[1])
        if f_key in filters_and_sorts:
            filters_and_sorts[f_key] = f_value
    final_line = dict(final_line, **filters_and_sorts)
    return final_line
