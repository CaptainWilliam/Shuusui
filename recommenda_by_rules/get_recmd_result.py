#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from models.md5_model import md5_model_0001
import logging

__author__ = 'LH Liu'

logger = logging.getLogger()


def get_recmd_result_by_running_models(hive_cmd_env, requirements, score_params, models, recmd_model_key):
    if recmd_model_key not in models:
        recmd_model_key = 'default'
    chosen_model = models.get(recmd_model_key)
    model_func, model_params = chosen_model.get('model_name'), chosen_model.get('params')
    recmd_result = run_model(hive_cmd_env, requirements, score_params, model_func, model_params)
    return recmd_result


def run_model(hive_cmd_env, requirements, score_params, model_func, model_params):
    if model_func and model_params:
        model_func_obj = eval(model_func)
        if model_func_obj in [md5_model_0001, ]:
            return model_func_obj(hive_cmd_env, requirements, score_params, model_params)
        else:
            raise ValueError('No model: {}'.format(model_func))
    else:
        raise ValueError('No {} or {}'.format(model_func, model_params))
