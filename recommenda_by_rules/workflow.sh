#!/usr/bin/env bash
log_folder_path='XXX/logs'
# log_folder_path='logs'
log_level='DEBUG'
working_dir='XXX'
hive_cmd_env='XXX/hive'
cd ${working_dir}
# source ./environment_config.sh
python_cmd_env = '/usr/local/bin/python'
${python_cmd_env} ./main.py $@ --log_folder_path=${log_folder_path} --log_level=${log_level} --hive_cmd_env=${hive_cmd_env}
if [ $? != 0 ] ; then
    exit 1
fi
exit 0