#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import hashlib
import logging

__author__ = 'LH Liu'

logger = logging.getLogger()


def get_recmd_model_key_by_running_model_selector(requirements, model_selector):
    try:
        input_value_field = model_selector.get('input_value')
        input_value = requirements.get(input_value_field)
        if input_value:
            selector_params = {'input_value': input_value, 'partition_method': model_selector.get('partition_method')}
            recmd_model_key = run_model_selector(model_selector.get('selector_name'), selector_params)
        else:
            recmd_model_key = 'default'
    except Exception, e:
        recmd_model_key = 'default'
        logger.info(e.message)
    return recmd_model_key


def run_model_selector(selector_func, selector_params):
    if selector_func and selector_params:
        selector_func_obj = eval(selector_func)
        return selector_func_obj(selector_params['input_value'], selector_params['partition_method'])
    else:
        raise TypeError('No selector func and selector params.')


def md5_mod_selector(input_value, partition_method):
    if input_value is None or partition_method is None or len(partition_method) == 0:
        raise
    if ('mod_key' not in partition_method or 'partitions' not in partition_method
            or not isinstance(partition_method.get('partitions'), dict)):
        raise
    mod_key = int(partition_method.get('mod_key'))
    partitions = partition_method.get('partitions')
    input_value_digest = hashlib.md5(str(input_value.encode('utf-8')))
    input_value_digest_integer = int(input_value_digest.hexdigest(), 16)
    mod_residual = str(int(input_value_digest_integer) % int(mod_key))
    if mod_residual in partitions:
        return partitions[mod_residual]
    else:
        if 'default' in partitions:
            return partitions['default']
        else:
            raise
