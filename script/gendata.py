# usage:
# python gendata.py
#   number of stripes
#   code [Clay|RSPIPE]
#   n
#   k
#   w
#   blockMiB
#   packetKiB
#   gen_files [true|false]
#   gen_blocks [true|false]
#   gen_meta [true|false]
# 
# 
# NOTE: before running the script, you should manually set OEC and Hadoop configs (including ip, block sizes, packet sizes and etc.)

import os
import random
import sys
import subprocess
import time

def usage():
    print("python gendata.py num_stripes code[RSPIPE|Clay] n k w blockMiB packetKiB gen_files[true|false] gen_blocks[true|false] gen_meta[true|false]")


if len(sys.argv) != 11:
    usage()
    exit()

NSTRIPES=int(sys.argv[1])+1
CODE=sys.argv[2]
ECN=int(sys.argv[3])
ECK=int(sys.argv[4])
ECW=int(sys.argv[5])
BLOCKMB=int(sys.argv[6])
PACKETKB=int(sys.argv[7])
gen_files=(sys.argv[8] == "true")
gen_blocks=(sys.argv[9] == "true")
gen_meta=(sys.argv[10] == "true")



# NOTE: fill in ips of the cluster
clusternodes=[
    "192.168.1.1",
    "192.168.1.2",
    "192.168.1.3",
    "192.168.1.4",
    "192.168.1.5",
    "192.168.1.6",
    "192.168.1.7",
    "192.168.1.8",
    "192.168.1.9",
    "192.168.1.10",
    "192.168.1.11",
    "192.168.1.12",
    "192.168.1.13",
    "192.168.1.14",
    "192.168.1.15",
    "192.168.1.16",
    "192.168.1.17",
    "192.168.1.18",
    "192.168.1.19",
    "192.168.1.20",
    "192.168.1.21",
    "192.168.1.22",
]

controller=clusternodes[0]
datanodes=clusternodes[1:-2]
clientnodes=clusternodes[-1]

# sizes
filesize_MB = BLOCKMB * ECK
block_bytes = BLOCKMB * 1048576
packet_bytes = PACKETKB * 1024


# home dir
cmd = r'echo ~'
home_dir_str, stderr = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
home_dir = home_dir_str.decode().strip()
# proj dir
proj_dir="{}/SPDist".format(home_dir)
script_dir = "{}/script".format(proj_dir)
block_dir="{}/blkDir".format(proj_dir)
stripestore_dir="{}/stripeStore".format(proj_dir)

# oec and hadoop dir
oec_proj_dir="{}/openec".format(home_dir)
oec_script_dir = "{}/script".format(oec_proj_dir)
start_oec_filename = "start.py"
stop_oec_filename = "stop.py"




# generate n files
if gen_files == True:
    for i in range(NSTRIPES):
        # go to the first OEC agent node
        cmd = "ssh {} \"cd {}; dd if=/dev/urandom of=input_{}_{} bs={}MiB count={} iflag=fullblock\"".format(
            datanodes[0], oec_proj_dir,
            str(filesize_MB), str(i),
            str(BLOCKMB), str(ECK),
        )
        print(cmd)
        os.system(cmd)



if gen_blocks == True:
    # NOTE: before running the script, you should manually set OEC and Hadoop configs (including ip, block sizes, packet sizes and etc.)

    # start HDFS
    cmd = "start-dfs.sh"
    print(cmd)
    os.system(cmd)

    # start OpenEC
    cmd = "cd {}; python {}".format(oec_script_dir, start_oec_filename)
    print(cmd)
    os.system(cmd)
    time.sleep(2)


    # generate NSTRIPES
    for i in range(NSTRIPES):
        OEC_code = ""
        if CODE == "RSPIPE":
            OEC_code = "RSCONV"
        else:
            OEC_code = CODE
        # go to the first client node
        cmd = "ssh {} \"cd {}; ./OECClient write input_{}_{} /{}-{}-{} {}_{}_{} online {}\"".format(
            datanodes[0], oec_proj_dir, 
            str(filesize_MB), str(i),
            CODE, str(BLOCKMB), str(i),
            OEC_code, str(ECN), str(ECK),
            str(filesize_MB))
        print(cmd)
        os.system(cmd)

        time.sleep(2)
        
        # run redis flushall between each two runs
        print("flush redis for {}-th stripe".format(str(i)))
        for node_ip in clusternodes:
            cmd = "ssh " + node_ip + " \"redis-cli flushall\""
            # print(cmd)
            os.system(cmd)

        time.sleep(2)


    # stop OpenEC
    cmd = "cd {}; python {}".format(oec_script_dir, stop_oec_filename)
    print(cmd)
    os.system(cmd)


if gen_meta == True:
    # generate NSTRIPES metadata for ParaRC
    for i in range(NSTRIPES):
        cmd = "cd {}; python3 dumpmeta.py -code {} -n {} -k {} -w {} -bs {} -ps {} -filename /{}-{}-{} -meta_filename {}-{}-{}".format(
            script_dir, CODE, ECN, ECK, ECW,
            block_bytes, packet_bytes, 
            CODE, BLOCKMB, i,
            CODE, BLOCKMB, i,
        )
        print(cmd)
        os.system(cmd)
