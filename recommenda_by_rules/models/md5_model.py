#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from table_schema import MEIPAI_ACCOUNT_FEATURE, MEIPAI_ACCOUNT_TEXT_CLASSIFICATION
from score_funcs import calculate_watch_score, calculate_category_score, calculate_engagement_score
import pandas as pd
import commands
import logging

__author__ = 'LH Liu'

logger = logging.getLogger()


def md5_model_0001(hive_cmd_env, requirements, score_params, model_params):
    # get some hql filters from the requirement
    categories = get_categories_from_requirements(requirements.get('category'))
    filters = get_filters_from_requirements(requirements.get('filters'))
    # get hql and run it in hive, then return recmd results as DataFrame
    recmd_results_path = get_matching_account_path_from_hive(hive_cmd_env, categories, filters)
    col_name = ['XXX', 'YYY', ]
    recmd_results_without_score = pd.read_csv(recmd_results_path, encoding='utf-8', index_col=None, names=col_name,
                                              sep='\t')
    # rescore the recmd recmd results
    recmd_results = rescore(recmd_results_without_score, score_params)
    output_recmd_results(recmd_results, model_params)
    return recmd_results


def get_categories_from_requirements(category):
    if isinstance(category, list):
        return '(\'{}\')'.format('\', \''.join(category))
    else:
        raise


def get_filters_from_requirements(filters):
    filters_hql_list = []
    for filter_name, filter_hql in filters.items():
        filters_hql_list.append('( ' + str(filter_hql.get('hql')) + ')')
    return ' and '.join(filters_hql_list)


def get_matching_account_path_from_hive(hive_cmd_env, categories, filters):
    basic_layer_hql = ('select *, row_number() over (partition by {}.{} order by {}.{} desc) as od '
                       'from {}.{}').format('1', '2', '3', '4', '5', categories, filters)

    # get the completed hql from the small hql parts
    return run_hql_in_hive(hive_cmd_env, basic_layer_hql)


def run_hql_in_hive(hive_cmd_env, hql):
    tmp_store_path = 'xxx/recmd_results.csv'
    status, output = commands.getstatusoutput('{} -S -e "{}" > {}'.format(hive_cmd_env, hql, tmp_store_path))
    if status:
        raise IOError('Hql executed failed.')
    return tmp_store_path


def rescore(recmd_results_without_score, score_params):
    pd_without_na = fill_na(recmd_results_without_score)
    recmd_results_with_score = add_segmented_score(pd_without_na, score_params)
    final_recmd_results = add_total_score_and_rank(recmd_results_with_score, score_params)
    return final_recmd_results


def fill_na(pd_with_na):
    # todo: can be config
    # pd_with_na.fillna({'name': 'allen', 'age': 18}, inplace=True) for example
    return pd_with_na


def add_segmented_score(pd_without_na, score_params):
    for score_item, col_and_fun_mapping in score_params.items():
        score_item_name = col_and_fun_mapping.get('pd_col')
        cal_func = col_and_fun_mapping.get('func_name')
        put_back_ward_col = col_and_fun_mapping.get('put_backward')
        rename = col_and_fun_mapping.get('rename')
        # from func name eval real func and make sure it is in memo
        real_cal_func = eval(cal_func)
        if real_cal_func not in [calculate_watch_score, calculate_category_score, calculate_engagement_score]:
            continue
        if isinstance(score_item_name, list):
            cal_func_param = eval(' + '.join(['pd_without_na[\'{}\']'.format(cn) for cn in score_item_name]))
            add_score_col = cal_func_param.apply(real_cal_func)
            if isinstance(put_back_ward_col, list):
                pd_without_na[rename] = eval(
                    ' + '.join(['pd_without_na[\'{}\']'.format(cn) for cn in put_back_ward_col]))
            else:
                pd_without_na[rename] = pd_without_na[put_back_ward_col]
            pd_without_na['score_' + rename] = add_score_col
        else:
            cal_func_param = pd_without_na[score_item_name]
            add_score_col = cal_func_param.apply(real_cal_func)
            if isinstance(put_back_ward_col, list):
                pd_without_na[rename] = eval(
                    ' + '.join(['pd_without_na[\'{}\']'.format(cn) for cn in put_back_ward_col]))
            else:
                pd_without_na[rename] = pd_without_na[put_back_ward_col]
            pd_without_na['score_' + rename] = add_score_col
    return pd_without_na


def add_total_score_and_rank(pd_with_score, score_params):
    score_name_list = []
    for score_item, col_and_fun_mapping in score_params.items():
        score_name_list.append('score_' + col_and_fun_mapping.get('rename'))
    pd_with_score['total_score'] = eval(
        ' + '.join(['pd_with_score[\'{}\']'.format(score_name) for score_name in score_name_list]))
    pd_with_score = pd_with_score.sort_values(by='total_score', ascending=False)
    return pd_with_score


def output_recmd_results(final_pd, model_params):
    output_path = 'xxx/recmd_results.csv'
    sep_str = str('\t')
    final_pd.index = range(1, final_pd.shape[0] + 1)
    final_pd[0: int(model_params.get('count'))].to_csv(output_path, sep=sep_str, encoding='utf-8')
