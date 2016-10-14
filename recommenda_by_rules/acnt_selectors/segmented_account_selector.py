# !/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
# from common.errors import *
# from utils.config_loader import load_log_config
from plot_distribution_for_selected_result import get_data_hist_distribution
import pandas as pd
import numpy as np
# import logging.config
# import logging
import hashlib
import random

__author__ = 'LH Liu'


# logging.config.dictConfig(load_log_config())
# g_logger = logging.getLogger('model_internal_logger')


def segmented_account_selector(company_id, recmd_result, selector_params, count=None):
    """

    :param company_id:
    :param recmd_result:
    :param selector_params:
    :param count:
    :return:
    """
    try:
        # get selector params
        if count is None:
            count = selector_params.get('count')
        segmented_ratio = selector_params.get('segmented_ratio')
        assigned_nums = selector_params.get('assigned_nums')

        # change list to dataframe(convenient for select and plot distribution)
        tmp_dataframe = check_data_type(recmd_result)

        # set random seed for each company so that the each user got the different results
        set_random_seed(company_id)

        if count and segmented_ratio and assigned_nums:
            selected_order = select_from_different_segmented_seq(tmp_dataframe.index, count, segmented_ratio,
                                                                 assigned_nums)
        else:
            # raise ApiArgumentMissingError('no assigned_nums in selector_params')
            raise TypeError
        # np use (np.col.isin(list)) and python generally use (col in list)
        selected_result = tmp_dataframe[np.where(tmp_dataframe.index.isin(selected_order), True, False)]

        get_data_hist_distribution(selected_result, 'total_score')

        return selected_result.values.tolist()

    except TypeError as e:
        pass

    except IndexError as e:
        pass
    '''
    except SelectorRunnerError as sre:
        g_logger.error('Account Selector Error: {}'.format(sre.message))

    except ApiArgumentMissingError as aame:
        g_logger.error('Account Selector Error: {}'.format(aame.message))
    '''


def check_data_type(recmd_result):
    """

    :param recmd_result:
    :return:
    """
    if isinstance(recmd_result, list):
        return pd.DataFrame(recmd_result, index=range(0, len(recmd_result)))
    elif isinstance(recmd_result, dict):
        return pd.DataFrame(recmd_result)
    elif isinstance(recmd_result, pd.DataFrame):
        return recmd_result
    else:
        # raise SelectorRunnerError('input data type is not list, dict or pd.Dataframe')
        raise TypeError


def set_random_seed(company_id):
    """

    :param company_id:
    :return:
    """
    if company_id is None:
        # raise ApiArgumentMissingError('no company_id used for account selector')
        raise TypeError
    # use company_id as random seed to make sure every company has different recmd results
    tmp_key = hashlib.md5(str(company_id.encode('utf-8')))
    unique_key_for_company_id = int(tmp_key.hexdigest(), 16)
    random.seed(unique_key_for_company_id)


# todo:if count is way too small but still bigger than total assigned nums, then do not use this model
def select_from_different_segmented_seq(index_seq, count, segmented_ratio, assigned_nums):
    """

    :param index_seq:
    :param count:
    :param segmented_ratio:
    :param assigned_nums:
    :return:
    """
    # get segmented seq's index:
    index_of_segmented_seq = get_the_index_of_segmented_seq(index_seq, segmented_ratio)
    adjusted_assigned_nums = adjust_assigned_nums_to_fit_count(count, assigned_nums)
    # pick up in each segmented dataset randomly
    selected_order = []
    for i in range(0, len(adjusted_assigned_nums)):
        each_segmented_seq = index_seq[index_of_segmented_seq[i]: index_of_segmented_seq[i + 1]]
        # todo: optimize it
        if len(each_segmented_seq) < adjusted_assigned_nums[i]:
            adjusted_assigned_nums[i + 1] = adjusted_assigned_nums[i] + adjusted_assigned_nums[i + 1] \
                                            - len(each_segmented_seq)
            adjusted_assigned_nums[i] = len(each_segmented_seq)
        selected_order += random.sample(each_segmented_seq, adjusted_assigned_nums[i])
    return selected_order


def get_the_index_of_segmented_seq(index_seq, segmented_ratio):
    """

    :param index_seq:
    :param segmented_ratio:
    :return:
    """
    index_seq_len, segmented_ratio_len = len(index_seq), len(segmented_ratio)
    if index_seq_len <= segmented_ratio_len:
        raise IndexError
    # normalized segmented ratio
    segmented_ratio_sum = sum(segmented_ratio)
    normalized_segmented_ratio = [1.0 * ratio / segmented_ratio_sum for ratio in segmented_ratio]
    index_of_segmented_seq = [0]
    for ratio in normalized_segmented_ratio:
        step = 1 if int(index_seq_len * ratio) == 0 else int(index_seq_len * ratio)
        index_of_segmented_seq.append(index_of_segmented_seq[-1] + step)
    # make sure the last one is the last number
    if index_of_segmented_seq[-1] is not index_seq_len - 1 and len(index_of_segmented_seq) == segmented_ratio_len + 1:
        index_of_segmented_seq[-1] = index_seq_len - 1
    # index_of_segmented_seq
    return index_of_segmented_seq


def adjust_assigned_nums_to_fit_count(count, assigned_nums):
    """

    :param count:
    :param assigned_nums:
    :return:
    """
    # this segment has no assigned nums
    if count < len(assigned_nums):
        raise IndexError
    # normalize the assigned_nums
    assigned_nums_sum = sum(assigned_nums)
    normalized_segmented_ratio = [
        int(count * assigned_num / assigned_nums_sum) if int(count * assigned_num / assigned_nums_sum) > 0 else 1
        for assigned_num in assigned_nums
        ]
    beautified_segmented_ratio = beautify_distribution(count, normalized_segmented_ratio)
    return beautified_segmented_ratio


def beautify_distribution(count, normalized_segmented_ratio):
    """

    :param count:
    :param normalized_segmented_ratio:
    :return:
    """
    small_num, big_num, total_nums = 0, len(normalized_segmented_ratio) - 1, sum(normalized_segmented_ratio)
    while sum(normalized_segmented_ratio) > count:
        if normalized_segmented_ratio[small_num] > 1:
            normalized_segmented_ratio[small_num] -= 1
        small_num = (small_num + 1) % len(normalized_segmented_ratio)
    while sum(normalized_segmented_ratio) < count:
        normalized_segmented_ratio[big_num] += 1
        big_num = (big_num - 1) % len(normalized_segmented_ratio)
    return normalized_segmented_ratio

# test
# select_from_different_segmented_seq(range(0, 1750), 200, [1, 11, 11, 11, 11, 11, 11, 11, 11, 11],
#                                     [4, 6, 8, 10, 12, 12, 12, 12, 12, 12])
