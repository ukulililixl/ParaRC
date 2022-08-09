#!/bin/bash

home_dir=$(echo ~)
proj_dir=${home_dir}/SPDist
config_dir=${proj_dir}/conf
script_dir=${proj_dir}/script
config_filename=sysSetting.xml

echo "controller"
cat ${config_dir}/${config_filename}

for i in {1..18}; do echo "data node $i"; ssh dn$i cat ${config_dir}/${config_filename}; done


