#!/bin/bash

home_dir=$(echo ~)
proj_dir=${home_dir}/SPDist
config_dir=${proj_dir}/conf
script_dir=${proj_dir}/script
config_filename=sysSetting.xml
controller_log_filename=coor_output
agent_log_filename=agent_output

for i in {1..18}; do echo "data node $i"; ssh dn$i "tail -n 10 ${proj_dir}/${agent_log_filename}"; done

echo "coordinator"
tail -n 10 ${proj_dir}/${controller_log_filename}

