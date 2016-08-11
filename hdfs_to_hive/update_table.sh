#!/usr/bin/env bash
log_folder_path='logs'
log_level='DEBUG'
working_dir='/var/weiboyi/azkaban/HdfsToHive/'
hadoop_cmd_env='/usr/local/hadoop/bin/hadoop'
hive_cmd_env='/usr/local/apache-hive-1.2.0/bin/hive'
hive_conf_path="/data0/weiboyi/azkaban/HdfsToHive/hdfs_to_hive.yml"
hive_store_folder_path='/user/hive/warehouse/external_table_data/'
cd ${working_dir}
source ./environment_config.sh
# python_cmd_env = '/usr/local/bin/python'
${python_cmd_env} ./main.py $@ --log_folder_path=${log_folder_path} --log_level=${log_level} --hadoop_cmd_env=${hadoop_cmd_env} --hive_cmd_env=${hive_cmd_env} --hive_conf_path=${hive_conf_path} --hive_store_folder_path=${hive_store_folder_path}
if [ $? != 0 ] ; then
    exit 1
fi
exit 0