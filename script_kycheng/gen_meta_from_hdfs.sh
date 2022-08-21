# Default configs
CLUSTER=csencs1
ECN=14
ECK=10
BLOCKMB=256
PKTKB=64
num_runs=10

# paths
home_dir=$(echo ~)

# project paths
proj_dir=${home_dir}/SPDist
config_dir=${proj_dir}/conf
script_dir=${proj_dir}/script
script_kycheng_dir=${proj_dir}/script_kycheng
config_filename=sysSetting.xml

# test paths
test_dir=${home_dir}/spdisttest/spdisttest_kycheng
exp_dir=${test_dir}/E3
data_dir=${test_dir}/data
cluster_dir=${test_dir}/cluster/${CLUSTER}
network_dir=${test_dir}/network
cache_dir=${test_dir}/cache
gen_config_dir=${test_dir}/conf

# calculation
BLOCK_B = `echo "${BLOCKMB} * 1048576" | bc -l`
PKT_B = `echo "${PKTKB} * 1024" | bc -l`

CODE=RSPIPE
ECW=1

cd ${script_kycheng_dir}
for i in $(seq 1 ${ECN}); do
    filename = /test + "-" + ${BLOCKMB} + "-" + ${i}
    meta_filename = test + "-" + ${BLOCKMB} + "-" + ${i}

    python3 gen_meta_from_hdfs.py -code ${CODE} -n ${ECN} -k ${ECK} -w ${ECW} -bs ${BLOCK_B} -ps ${PKT_B} -filename ${filename} -meta_filename ${meta_filename}

    echo "finished generation of meta file: " + ${meta_filename}