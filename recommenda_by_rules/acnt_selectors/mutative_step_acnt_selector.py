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


def mutative_step_acnt_selector(recmd_result, selector_params, company_id):
    tmp_dataframe = check_data_type(recmd_result)
    set_random_seed(company_id)
    if 'assigned_nums' in selector_params and 'assigned_steps' in selector_params:
        selected_order = select_from_each_segmented_seq(tmp_dataframe.index, selector_params)
    else:
        raise
    # np use (np.col.isin(list)) and python generally use (col in list)
    selected_result = tmp_dataframe[np.where(tmp_dataframe.index.isin(selected_order), True, False)]
    get_data_hist_distribution(selected_result, 'total_score')
    return selected_result


def select_from_each_segmented_seq(index_seq, selector_params):
    # even spilt the dataset
    assigned_nums, assigned_steps = selector_params.get('assigned_nums'), selector_params.get('assigned_steps')
    upper_limit = int(len(list(index_seq)))
    mu_step = int(upper_limit // (len(assigned_nums)) + 1)
    segmented_sql_index = range(0, upper_limit, mu_step)
    # make sure the last one is the last number
    if len(segmented_sql_index) == len(assigned_nums) and segmented_sql_index[-1] != upper_limit:
        segmented_sql_index.append(upper_limit - 1)
    # pick up in each segmented dataset by mutative steps
    selected_order = []
    for k in range(0, len(assigned_nums)):
        selected_order += select_with_mutative_steps(index_seq[segmented_sql_index[k]: segmented_sql_index[k + 1]],
                                                     int(assigned_nums[k]), assigned_steps[k])
    return selected_order


def select_with_mutative_steps(index_seq, assigned_num, assigned_step):
    if len(index_seq) < assigned_num:
        raise
    selected_index = set()
    selected_time, total_step = 0, 0
    while len(selected_index) != assigned_num:
        basic_step = random.randint(0, 20)
        if len(selected_index) >= assigned_num:
            total_step = (total_step + assigned_step[-1] * basic_step + index_seq[0]) % index_seq[-1]
            selected_index.add(index_seq[0] + total_step)
        else:
            total_step = (total_step + assigned_step[selected_time] * basic_step + index_seq[0]) % index_seq[-1]
            selected_index.add(index_seq[0] + total_step)
        selected_time += 1
    return list(selected_index)
