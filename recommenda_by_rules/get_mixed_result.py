#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from acnt_selectors.md5_acnt_selector import md5_acnt_selector_0001
import logging

__author__ = 'LH Liu'

logger = logging.getLogger()


def get_mixed_result_by_running_acnt_selector(recmd_result, acnt_selectors, selector_key):
    if selector_key not in acnt_selectors:
        selector_key = 'default'
    chosen_selector = acnt_selectors.get(selector_key)
    selector_func, selector_params = chosen_selector.get('model_name'), chosen_selector.get('params')
    mixed_result = run_selector(recmd_result, selector_func, selector_params)
    return mixed_result


def run_selector(recmd_result, selector_func, selector_params):
    if selector_func:
        selector_func_obj = eval(selector_func)
        if selector_func_obj in [md5_acnt_selector_0001, ]:
            return selector_func_obj(recmd_result, selector_params)
        else:
            raise ValueError('Selector {} does not define'.format(selector_func))
    else:
        raise ValueError('No {}'.format(selector_func))
