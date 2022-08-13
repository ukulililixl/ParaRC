#!/bin/bash

home_dir=$(echo ~)
proj_dir=${home_dir}/SPDist
config_dir=${proj_dir}/conf
script_dir=${proj_dir}/script
block_dir=${proj_dir}/blkDir
config_filename=sysSetting.xml


for i in {1..18}; do echo "data node $i"; ssh dn${i} "ls -l ${block_dir}"; done


