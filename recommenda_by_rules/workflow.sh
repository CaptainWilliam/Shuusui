#!/usr/bin/env bash
log_folder_path='logs'
log_level='DEBUG'
working_dir='XXX/dake_recommendation'
hive_cmd_env='XXX/bin/hive'
conf_path='XXX/model_selectors_and_models.yaml'
cd ${working_dir}
# source ./environment_config.sh
python_cmd_env = 'XXXl/bin/python'
${python_cmd_env} ./main.py $@ --log_folder_path=${log_folder_path} --log_level=${log_level} --hadoop_cmd_env=${hadoop_cmd_env} --conf_path=${conf_path}
if [ $? != 0 ] ; then
    exit 1
fi
exit 0