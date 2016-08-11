#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

__author__ = 'LH Liu'


def get_enum_value(second_parsed_line, completed_item_map):
    for enum_key, enum_value in completed_item_map.items():
        second_parsed_line[enum_key] = enum_value.get(second_parsed_line[enum_key], '')
    return second_parsed_line
