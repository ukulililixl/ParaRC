#!/bin/bash

home_dir=$(echo ~) 
proj_dir=${home_dir}/SPDist
config_dir=${proj_dir}/conf
script_dir=${proj_dir}/script 
config_filename=sysSetting.xml

my_ip=$(ifconfig | grep 192.168.10 | head -1 | sed "s/ *inet [addr:]*\([^ ]*\).*/\1/")

sed -i "s%\(<attribute><name>local.addr</name><value>\)[^<]*%\1${my_ip}%" ${config_dir}/${config_filename}
#sed -i "s%\(<property><name>oec.local.addr</name><value>\)[^<]*%\1${my_ip}%" /home/openec/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/hdfs-site.xml
