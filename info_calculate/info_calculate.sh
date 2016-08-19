#!/usr/bin/env bash
log_folder='/var/weiboyi/azkaban/MuggleSearchEngine/log/hive_info_calculate_and_upload_to_es/logs'
log_level='DEBUG'
working_dir='/var/weiboyi/azkaban/MuggleSearchEngine/log/hive_info_calculate_and_upload_to_es'
hive_cmd_env='/usr/local/apache-hive-1.2.0/bin/hive'
cd ${working_dir}
source ./environment_config.sh
${python_cmd_env} ./main.py $@ --log_folder_path=${log_folder} --log_level=${log_level} --hive_cmd_env=${hive_cmd_env}
if [ $? != 0 ] ; then
    exit 1
fi
exit 0