#!/usr/bin/python
import sys
import subprocess
import os
import argparse
import re

def parse_args(cmd_args):
    arg_parser = argparse.ArgumentParser(description="Generate block metadata files from HDFS by OpenEC online write in XML format")
    arg_parser.add_argument("-n", type=int, required=True, help="ECN")
    arg_parser.add_argument("-k", type=int, required=True, help="ECK")
    arg_parser.add_argument("-bs", type=int, required=True, help="EC block size (in Bytes)")
    arg_parser.add_argument("-filename", type=str, required=True, help="file name (prefix) from OpenEC")

    args = arg_parser.parse_args(cmd_args)
    return args

def get_hdfs_block_meta():
    pass

def get_home_dir():
    # home dir
    cmd = r'echo ~'
    home_dir_str, stderr = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
    home_dir = home_dir_str.decode().strip()
    return home_dir

def get_hadoop_home_dir():
    # home dir
    cmd = r'echo $HADOOP_HOME'
    hadoop_home_dir_str, stderr = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
    hadoop_home_dir = hadoop_home_dir_str.decode().strip()
    return hadoop_home_dir

def get_blk_records(filename, hadoop_home_dir):
    # list all blocks
    block_map = {}
    block_names = []
    cmd = "hdfs fsck / -files -blocks -locations | grep {}".format(filename)
    block_records = os.popen(cmd).readlines()
    for block_record in block_records:
        match_results = re.match(r"^(\S+) .*$", block_record)
        block_name = match_results.groups()[0]
        block_names.append(block_name)

    block_paths = []
    cmd = "hdfs fsck / -files -blocks -locations | grep Datanode"
    block_records = os.popen(cmd).readlines()
    for block_record in block_records:
        match_results = re.match(r"^.*BP-([0-9.-]+):blk_(\d+)_.*$", block_record)
        folder_name_suffix = match_results.groups()[0]
        block_name_suffix = match_results.groups()[1]

        block_path = hadoop_home_dir + "/dfs/data/current/BP-" + folder_name_suffix + "/current/finalized/subdir0/subdir0/blk_" + block_name_suffix

        block_paths.append(block_path)

    print(block_names)
    print(block_paths)

    match_string = "^{}_oecobj_([\d+])$".format(filename)
    print(match_string)
    for i in range(len(block_names)):
        match_results = re.match(match_string, block_names[i])
        if not match_results:
            continue
        block_id = int(match_results.groups()[0])
        block_map[block_id] = [block_names[i], block_paths[i]]

    return block_map


def main():
    args = parse_args(sys.argv[1:])
    if not args:
        exit()
    
    ecn = args.n
    eck = args.k
    bs_Bytes = args.bs
    filename = args.filename

    home_dir = get_home_dir()

    # proj dir
    proj_dir="{}/SPDist".format(home_dir)
    script_dir = "{}/script_kycheng".format(proj_dir)
    config_dir = "{}/conf".format(proj_dir)
    config_filename = "sysSetting.xml"
    blk_dir = "{}/blkDir".format(proj_dir)
    stripeStore_dir = "{}/stripeStore".format(proj_dir)

    # Hadoop home dir
    hadoop_home_dir = get_hadoop_home_dir()
    
    # get block map <block_id, [block_name, block_path]>
    block_map = get_blk_records(filename, hadoop_home_dir)

    # generate xml file to block records (check filename)



if __name__ == '__main__':
    main()