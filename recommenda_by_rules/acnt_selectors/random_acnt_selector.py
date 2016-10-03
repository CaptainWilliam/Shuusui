# !/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from plot_distribution_for_selected_result import get_data_hist_distribution
import pandas as pd
import numpy as np
import hashlib
import logging
import random

__author__ = 'LH Liu'

logger = logging.getLogger()


def random_acnt_selector(recmd_result, selector_params, company_id):
    tmp_dataframe = check_data_type(recmd_result)
    set_random_seed(company_id)
    if 'count' in selector_params:
        selected_order = random_select_from_seq(tmp_dataframe.index, selector_params.get('count'))
    else:
        selected_order = random_select_from_seq(tmp_dataframe.index, 100)
    # np use (np.col.isin(list)) and python generally use (col in list)
    selected_result = tmp_dataframe[np.where(tmp_dataframe.index.isin(selected_order), True, False)]
    get_data_hist_distribution(selected_result, 'total_score')
    return selected_result


def check_data_type(recmd_result):
    if isinstance(recmd_result, list):
        return pd.DataFrame(recmd_result, index=range(0, len(recmd_result)))
    elif isinstance(recmd_result, dict):
        return pd.DataFrame(recmd_result)
    elif isinstance(recmd_result, pd.DataFrame):
        return recmd_result
    else:
        raise


def set_random_seed(company_id):
    if company_id is None:
        raise
    # use company_id as random seed to make sure every company has different recmd results
    tmp_key = hashlib.md5(str(company_id.encode('utf-8')))
    unique_key_for_company_id = int(tmp_key.hexdigest(), 16)
    random.seed(unique_key_for_company_id)


def random_select_from_seq(index_seq, total_nums):
    return random.sample(index_seq, int(total_nums))
