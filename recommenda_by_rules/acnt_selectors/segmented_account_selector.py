# !/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from common.errors import *
from utils.config_loader import load_log_config
import logging.config
import logging
import hashlib
import random

__author__ = 'LH Liu'

logging.config.dictConfig(load_log_config())
g_logger = logging.getLogger('model_internal_logger')


def segmented_account_selector(company_id, recmd_result, self_define_count, selector_params):
    """

    :param company_id:
    :param recmd_result:
    :param self_define_count:
    :param selector_params:
    :return:
    """
    try:
        # get selector params
        print 'In selector'
        count = selector_params.get('count') if self_define_count is None else self_define_count
        if count >= len(recmd_result):
            return recmd_result
        segmented_ratio = selector_params.get('segmented_ratio')
        assigned_nums = selector_params.get('assigned_nums')
        # set random seed for each company so that the each user got the different results
        set_random_seed(company_id)
        # generate random seq(total as count)
        if count and segmented_ratio and assigned_nums:
            print 'Selector 1'
            select_order = select_from_different_segmented_seq(range(0, len(recmd_result)), count, segmented_ratio,
                                                                 assigned_nums)
            print 'Selector 2'
        else:
            raise ApiArgumentMissingError('No assigned_nums in selector_params')
        # np use (np.col.isin(list)) and python generally use (col in list)
        selected_result = select_by_order_from_recmd_result(recmd_result, select_order)
        print 'Selector 3'
        print selected_result
        return selected_result

    except IndexError as e:
        g_logger.error(e.message)
    except SelectorRunnerError as e:
        g_logger.error('Account Selector Error: {}'.format(e.message))
    except ApiArgumentMissingError as e:
        g_logger.error('Account Selector Error: {}'.format(e.message))
    except InternalParameterError as e:
        g_logger.error('Account Selector Error: {}'.format(e.message))


def set_random_seed(company_id):
    """

    :param company_id:
    :return:
    """
    if company_id is None:
        raise ApiArgumentMissingError('No company_id used for account selector')
    # use company_id as random seed to make sure every company has different recmd results
    tmp_key = hashlib.md5(str(company_id.encode('utf-8')))
    unique_key_for_company_id = int(tmp_key.hexdigest(), 16)
    random.seed(unique_key_for_company_id)


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
    # adjust ssigned nums in the mean time make sure the sum of assigned nums is the same as count
    adjusted_assigned_nums = adjust_assigned_nums_to_fit_count(count, assigned_nums)
    # change index_of_segmented_seq and adjusted_assigned_nums if their distribution does not match
    overflow_for_each_segmented_seq = 0
    for i in range(0, len(adjusted_assigned_nums)):
        single_segmented_seq = index_seq[index_of_segmented_seq[i]: index_of_segmented_seq[i + 1]]
        overflow_for_each_segmented_seq += adjusted_assigned_nums[i] - len(single_segmented_seq)
        overflow_for_each_segmented_seq = overflow_for_each_segmented_seq if overflow_for_each_segmented_seq > 0 else 0
    if overflow_for_each_segmented_seq > 0:
        index_of_segmented_seq, adjusted_assigned_nums = simple_binaryzation(index_of_segmented_seq,
                                                                             adjusted_assigned_nums)
    # pick up in each segmented dataset randomly with overflow
    select_order = []
    for i in range(0, len(adjusted_assigned_nums)):
        single_segmented_seq = index_seq[index_of_segmented_seq[i]: index_of_segmented_seq[i + 1]]
        if len(single_segmented_seq) < adjusted_assigned_nums[i]:
            adjusted_assigned_nums[i + 1] = adjusted_assigned_nums[i] + adjusted_assigned_nums[i + 1] - len(
                single_segmented_seq)
            adjusted_assigned_nums[i] = len(single_segmented_seq)
        segmented_seq_sample = random.sample(single_segmented_seq, adjusted_assigned_nums[i])
        select_order += segmented_seq_sample
        print segmented_seq_sample
    return select_order


def get_the_index_of_segmented_seq(index_seq, segmented_ratio):
    """

    :param index_seq:
    :param segmented_ratio:
    :return:
    """
    index_seq_len, segmented_ratio_len = len(index_seq), len(segmented_ratio)
    if index_seq_len <= segmented_ratio_len:
        raise IndexError('The length of recmd result are smaller than length of segmented ratio list')
    # normalized segmented ratio
    segmented_ratio_sum = sum(segmented_ratio)
    normalized_segmented_ratio = [1.0 * ratio / segmented_ratio_sum for ratio in segmented_ratio]
    index_of_segmented_seq = [0]
    for ratio in normalized_segmented_ratio:
        step = 1 if int(index_seq_len * ratio) == 0 else int(index_seq_len * ratio)
        index_of_segmented_seq.append(index_of_segmented_seq[-1] + step)
    # make sure the last one is the last number of the index seq
    if index_of_segmented_seq[-1] is not index_seq_len - 1 and len(index_of_segmented_seq) == segmented_ratio_len + 1:
        index_of_segmented_seq[-1] = index_seq_len - 1
    print index_of_segmented_seq
    return index_of_segmented_seq


def adjust_assigned_nums_to_fit_count(count, assigned_nums):
    """

    :param count:
    :param assigned_nums:
    :return:
    """
    # this segment has no assigned nums
    if count < len(assigned_nums):
        raise IndexError('The total return count is smaller than length of assigned nums list')
    # normalize the assigned_nums
    assigned_nums_sum = sum(assigned_nums)
    normalized_segmented_ratio = [
        int(count * assigned_num / assigned_nums_sum) if int(count * assigned_num / assigned_nums_sum) > 0 else 1 for
        assigned_num in assigned_nums]
    beautified_segmented_ratio = beautify_distribution(count, normalized_segmented_ratio)
    print beautified_segmented_ratio
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


def simple_binaryzation(index_seq, nums_seq):
    """

    :param index_seq:
    :param nums_seq:
    :return:
    """
    if not isinstance(index_seq, list) or not isinstance(nums_seq, list) or len(index_seq) != len(nums_seq) + 1:
        raise InternalParameterError('The index of each segment and the assigned nums are incorrect')
    return [index_seq[0], index_seq[1], index_seq[-1]], [nums_seq[0], sum(nums_seq) - nums_seq[0]]


def select_by_order_from_recmd_result(recmd_result, select_order):
    """

    :param recmd_result:
    :param select_order:
    :return:
    """
    sorted_recmd_accounts = sorted(recmd_result, key=lambda account: account['recmd_score'], reverse=True)
    for order in sorted(select_order):
        yield sorted_recmd_accounts[order]
