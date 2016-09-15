#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from get_recmd_model_key import get_recmd_model_key_by_running_model_selector
from models.md5_model import rescore
from utils.config_helper import ConfigHelper
import pandas as pd
import unittest

__author__ = 'LH Liu'


class TestModelSelector(unittest.TestCase):
    # add func
    def _list_equal(self, list_one, list_another):
        set_one = set(list_one)
        set_another = set(list_another)
        msg = '{} != {}'.format(list_one, list_another)
        if len(set_one) != len(set_another):
            raise self.failureException(msg)
        for element in set_one:
            if element not in set_another:
                raise self.failureException(msg)

    # add func
    def _list_in(self, func_results, ideal_results):
        set_ideal_results = set(ideal_results)
        if func_results not in set_ideal_results:
            msg = '{} not in {}'.format(func_results, ideal_results)
            raise self.failureException(msg)

    # tests
    def test_get_recmd_model_key(self):
        requirement = ConfigHelper.load_config(
            'E:\\PythonProject\\Work\\work_3\\dake_recommendation\\conf\\recmd_requirement.yaml'
        )
        model_selector = ConfigHelper.load_config(
            'E:\\PythonProject\\Work\\work_3\\dake_recommendation\\conf\\model_selectors_and_models.yaml'
        ).get('model_selector')
        results = ['md5_model_0001', 'md5_model_0002']
        self._list_in(get_recmd_model_key_by_running_model_selector(requirement, model_selector), results)

    def test_rescore(self):
        col_name = ['id', 'name', 'age', 'money']
        recmd_results_without_score = pd.read_csv(
            'E:\\PythonProject\\Work\\work_3\\dake_recommendation\\test\\test_1.txt', encoding='utf-8', names=col_name,
            sep=' ')
        score_params = ConfigHelper.load_config('E:\\PythonProject\\Work\\work_3\\dake_recommendation\\conf\\tmp.yaml')
        model_params = {}
        print rescore(recmd_results_without_score, score_params, model_params)


if __name__ == '__main__':
    unittest.main()
