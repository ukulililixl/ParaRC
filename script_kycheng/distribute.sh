#!/bin/bash

home_dir=$(echo ~)
proj_dir=${home_dir}/SPDist
config_dir=${proj_dir}/conf
script_dir=${proj_dir}/script
script_kycheng_dir=${proj_dir}/script_kycheng
config_filename=sysSetting.xml

num_nodes=18
for i in $(seq 1 ${num_nodes}); do
    # # copy the folder
    # scp -r ${proj_dir} dn${i}:${home_dir}
    
    # copy config file only
    # update IP for config files
    ssh dn${i} "mkdir -p ${config_dir}"
    scp ${config_dir}/${config_filename} dn${i}:${config_dir}
    ssh dn${i} "cd ${script_kycheng_dir} && bash set_config.sh"


    echo "finished for node ${i}"
done


