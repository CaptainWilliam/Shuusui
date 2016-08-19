#!/usr/bin/env bash
log_folder_path='logs'
log_level='DEBUG'
# todo: alter XXX
working_dir='/XXX/'
hadoop_cmd_env='/XXX/bin/hadoop'
hive_cmd_env='/XXX/bin/hive'
hive_conf_path="/XXX/XXX.yaml"
hive_store_folder_path='/XXX/'
cd ${working_dir}
source ./environment_config.sh
# python_cmd_env = '/usr/local/bin/python'
${python_cmd_env} ./main.py $@ --log_folder_path=${log_folder_path} --log_level=${log_level} --hadoop_cmd_env=${hadoop_cmd_env} --hive_cmd_env=${hive_cmd_env} --hive_conf_path=${hive_conf_path} --hive_store_folder_path=${hive_store_folder_path}
if [ $? != 0 ] ; then
    exit 1
fi
exit 0