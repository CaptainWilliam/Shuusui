# !/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from random_acnt_selector import check_data_type, set_random_seed
from plot_distribution_for_selected_result import get_data_hist_distribution
import numpy as np
import logging
import random

__author__ = 'LH Liu'

logger = logging.getLogger()


def segmented_acnt_selector(recmd_result, selector_params, company_id):
    tmp_dataframe = check_data_type(recmd_result)
    set_random_seed(company_id)
    if 'assigned_nums' in selector_params:
        selected_order = select_from_different_segmented_seq(tmp_dataframe.index, selector_params)
    else:
        raise
    # np use (np.col.isin(list)) and python generally use (col in list)
    selected_result = tmp_dataframe[np.where(tmp_dataframe.index.isin(selected_order), True, False)]
    get_data_hist_distribution(selected_result, 'total_score')
    return selected_result


def select_from_different_segmented_seq(index_seq, selector_params):
    assigned_nums = selector_params.get('assigned_nums')
    upper_limit = int(len(list(index_seq)))
    mu_step = int(upper_limit//(len(assigned_nums))+1)
    segmented_sql_index = range(0, upper_limit, mu_step)
    if len(segmented_sql_index) == len(assigned_nums) and segmented_sql_index[-1] != upper_limit:
        segmented_sql_index.append(upper_limit - 1)
    selected_order = []
    for i in range(0, len(assigned_nums)):
        selected_order += random.sample(index_seq[segmented_sql_index[i]: segmented_sql_index[i+1]], assigned_nums[i])
    return selected_order
