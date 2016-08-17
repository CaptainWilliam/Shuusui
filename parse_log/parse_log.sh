#!/usr/bin/env bash
log_folder='/var/weiboyi/azkaban/MSE/log/logs'
log_level='DEBUG'
working_dir='/var/weiboyi/azkaban/MSE/log/log_parse_to_info/'
input_path='/data/web_analytics_log/'
output_path='/data0/weiboyi/azkaban/MSE/log/log_parse_to_info/'
output_file_name='log_info'
output_file_type='.tsv'
cd ${working_dir}
source ./environment_config.sh
${python_cmd_env} ./main.py $@ --log_folder_path=${log_folder} --log_level=${log_level} --input_path=${input_path} --output_path=${output_path} --poutput_file_name=${output_file_name} --output_file_type=${output_file_type}
if [ $? != 0 ] ; then
    exit 1
fi
exit 0