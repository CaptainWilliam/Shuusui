#!/usr/bin/env bash
# set the virtualenvwrapper path
vitual_envwrapper_shell_path='/usr/local/bin/virtualenvwrapper.sh'
# set the virtualenv working directory
# todo: alter (path)
export WORKON_HOME='/var/(path)/.virtualenv'
# set the virtualenv name
# todo: alter (project_name)
virtualenv_name='(project_name)'
# source the virtualenvwrapper shell
if [ -r ${vitual_envwrapper_shell_path} ]; then
    source ${vitual_envwrapper_shell_path}
else
    echo "WARNING: Can't find virtualenvwrapper.sh"
fi
# change to the virtualenv
workon ${virtualenv_name}
# set the virtual python path
python_cmd_env=${WORKON_HOME}'/'${virtualenv_name}'/bin/python'