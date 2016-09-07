#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from models.md5_model import md5_model_001
import logging

__author__ = 'LH Liu'

logger = logging.getLogger()


def get_recmd_result_by_running_models(requirements, models, recmd_model_key):
    if recmd_model_key not in models:
        recmd_model_key = 'default'
    chosen_model = models.get(recmd_model_key)
    model_func, model_params = chosen_model.get('model_name'), chosen_model.get('params')
    recmd_result = run_model(requirements, model_func, model_params)
    return recmd_result


def run_model(requirements, model_func, model_params):
    if model_func and model_params:
        model_func_obj = eval(model_func)
        return model_func_obj(requirements, model_params)
    else:
        raise
