#!/usr/bin/python
import sys
import subprocess
import os
import argparse
import re
import xml.etree.ElementTree as ET

def parse_args(cmd_args):
    arg_parser = argparse.ArgumentParser(description="Generate block metadata files from HDFS by OpenEC online write in XML format")
    arg_parser.add_argument("-code", type=str, required=True, help="EC code name")
    arg_parser.add_argument("-n", type=int, required=True, help="ECN")
    arg_parser.add_argument("-k", type=int, required=True, help="ECK")
    arg_parser.add_argument("-w", type=int, required=True, help="ECW")
    arg_parser.add_argument("-bs", type=int, required=True, help="EC block size (in Bytes)")
    arg_parser.add_argument("-ps", type=int, required=True, help="EC packet size (in Bytes)")
    arg_parser.add_argument("-filename", type=str, required=True, help="file name (prefix) from OpenEC")
    arg_parser.add_argument("-meta_filename", type=str, required=True, help="metadata file name (prefix) for ParaRC")

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
    block_hdfs_filenames = []
    block_names = []
    block_ips = []
    block_paths = []
    cmd = "hdfs fsck / -files -blocks -locations | sed -n \'/{}/,/{}/p\'".format(filename[1:], "Datanode")
    block_records = os.popen(cmd).readlines()
    num_records = int(len(block_records) / 2)
    for i in range(num_records):
        block_record_filename = block_records[i*2]
        block_record_blockname = block_records[i*2+1]

        # block name
        match_results = re.match(r"^(\S+) .*$", block_record_filename)
        block_hdfs_filename = match_results.groups()[0]
        block_hdfs_filenames.append(block_hdfs_filename)

        # block location and path
        match_results = re.match(r"^.*BP-([0-9.-]+):blk_(\d+)_.*$", block_record_blockname)
        folder_name_suffix = match_results.groups()[0]
        block_name_suffix = match_results.groups()[1]

        match_results = re.match(r"^.*\[DatanodeInfoWithStorage\[([0-9.]+):.*$", block_record_blockname)
        block_ip = match_results.groups()[0]
        block_name = "blk_" + block_name_suffix

        block_names.append(block_name)
        block_ips.append(block_ip)


        # # find block path
        # block_root_dir = hadoop_home_dir + "/dfs/data/current"
        # cmd = 'ssh {} \'cd {}; find \"$(pwd -P)\" -name \"{}\"\''.format(block_ip, block_root_dir, block_name)
        # block_path_str, stderr = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
        # block_path = block_path_str.decode().strip()

        # block_path = hadoop_home_dir + "/dfs/data/current/BP-" + folder_name_suffix + "/current/finalized/subdir0/subdir0/blk_" + block_name_suffix

        # block_names.append(block_name)
        # block_ips.append(block_ip)
        # block_paths.append(block_path)

    # print(block_hdfs_filenames)
    # print(block_names)
    # print(block_paths)
    # print(block_ips)

    match_string = "^{}_oecobj_(\d+)$".format(filename)
    # match_string = "^{}_([\d+])_oecobj_0$".format(filename)
    for i in range(len(block_hdfs_filenames)):
        match_results = re.match(match_string, block_hdfs_filenames[i])
        if not match_results:
            continue
        block_id = int(match_results.groups()[0])
        block_map[block_id] = [block_names[i], block_ips[i]]
        # block_map[block_id] = [block_names[i], block_ips[i], block_paths[i]]

    return block_map

def add_xml_element(root, attr_name, attr_val):
    if root is None:
        print("error: invalid root")
        return None

    attr = ET.SubElement(root, "attribute")
    attr.tail = "\n"
    name = ET.SubElement(attr, "name")
    name.text = attr_name

    # special handling for blocklist
    if attr_name != "blocklist":
        value = ET.SubElement(attr, "value")
        value.text = str(attr_val)
    else:
        name.tail = "\n"
        for key, val in attr_val.items():
            value = ET.SubElement(attr, "value")
            value.text = str(val[0]) + ":" + str(val[1])
            value.tail = "\n"


def main():
    args = parse_args(sys.argv[1:])
    if not args:
        exit()

    code = args.code
    ecn = args.n
    eck = args.k
    ecw = args.w
    bs_Bytes = args.bs
    ps_Bytes = args.ps
    filename = args.filename
    meta_filename = args.meta_filename

    home_dir = get_home_dir()

    # proj dir
    proj_dir="{}/ParaRC".format(home_dir)
    script_dir = "{}/script".format(proj_dir)
    config_dir = "{}/conf".format(proj_dir)
    config_filename = "sysSetting.xml"
    blk_dir = "{}/blkDir".format(proj_dir)
    stripeStore_dir = "{}/stripeStore".format(proj_dir)

    # Hadoop home dir
    hadoop_home_dir = get_hadoop_home_dir()

    # get block map <block_id, [block_name, block_path]>
    block_map = get_blk_records(filename, hadoop_home_dir)

    # print(block_map)

    # generate xml file to block records (check filename)
    # Note: in HDFS, files are named starting with "/"
    meta_file_path = stripeStore_dir + "/" + filename[1:] + ".xml"

    # build XML file
    _stripe = ET.Element("stripe")
    _stripe.text = "\n"
    add_xml_element(_stripe, "code", code)
    add_xml_element(_stripe, "ecn", ecn)
    add_xml_element(_stripe, "eck", eck)
    add_xml_element(_stripe, "ecw", ecw)
    add_xml_element(_stripe, "stripename", filename[1:])
    add_xml_element(_stripe, "blocklist", block_map)
    add_xml_element(_stripe, "blockbytes", str(bs_Bytes))
    add_xml_element(_stripe, "pktbytes", str(ps_Bytes))

    tree = ET.ElementTree(_stripe)

    tree.write(meta_file_path)

    print("generate meta for file {} at {}".format(filename, meta_file_path))




if __name__ == '__main__':
    main()
