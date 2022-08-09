# usage
#   python createconf.py
#       1. cluster [office|aliyunhdd|surface|csencs1]

import os
import sys
import subprocess

def usage():
    print("python3 createconf.py cluster[office|aliyunhdd|surface|csencs1]")


if len(sys.argv) != 2:
    usage()
    exit()


CLUSTER=sys.argv[1]
ECCSIZE="10"
RPTHREADS="4"


# home dir
cmd = r'echo ~'
home_dir_str, stderr = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
home_dir = home_dir_str.decode().strip()
# test dir
test_dir="{}/spdisttest/spdisttest_kycheng".format(home_dir)
exp_dir=test_dir+"/E3"
data_dir=test_dir+"/data"
genconfig_dir="{}/conf".format(test_dir)
cluster_dir="{}/cluster/{}".format(test_dir, CLUSTER)

# proj dir
proj_dir="{}/SPDist".format(home_dir)
script_dir = "{}/script_kycheng".format(proj_dir)
config_dir = "{}/conf".format(proj_dir)
config_filename = "sysSetting.xml"
blk_dir = "{}/blkDir".format(proj_dir)
stripeStore_dir = "{}/stripeStore".format(proj_dir)
tradeoffPoint_dir = "{}/tradeoffPoint".format(proj_dir)



clusternodes=[]
controller=""
datanodes=[]
clientnodes=[]

# get controller
f=open(cluster_dir+"/dist_controller","r")
for line in f:
    controller=line[:-1]
    clusternodes.append(controller)
f.close()

# get datanodes
f=open(cluster_dir+"/dist_agents","r")
for line in f:
    agent=line[:-1]
    clusternodes.append(agent)
    datanodes.append(agent)
f.close()

# get clients
f=open(cluster_dir+"/dist_clients","r")
for line in f:
    client=line[:-1]
    clusternodes.append(client)
    clientnodes.append(client)
f.close()

print("controller", controller)
print("datanodes", datanodes)
print("clientnodes", clientnodes)
print("clusternodes", clusternodes)

# threads
controller_threads = 0
agent_threads = 0
cmddist_threads = 0
if CLUSTER == "office" or CLUSTER == "surface":
    controller_threads = 4
    agent_threads = 4
    cmddist_threads = 4
if CLUSTER == "aliyunhdd" or CLUSTER == "csencs1":
    controller_threads = 20
    agent_threads = 20
    cmddist_threads = 10

for node in clusternodes:

    content=[]

    line="<setting>\n"
    content.append(line)

    line="<attribute><name>controller.addr</name><value>"+controller+"</value></attribute>\n"
    content.append(line)

    line="<attribute><name>agents.addr</name>\n"
    content.append(line)

    for agent in datanodes:
        line="<value>/default/"+agent+"</value>\n"
        content.append(line)

    line="</attribute>\n"
    content.append(line)

    line="<attribute><name>clients.addr</name>\n"
    content.append(line)

    for client in clientnodes:
        line="<value>/default/"+client+"</value>\n"
        content.append(line)

    line="</attribute>\n"
    content.append(line)

    line="<attribute><name>controller.thread.num</name><value>"+str(controller_threads)+"</value></attribute>\n"
    content.append(line)

    line="<attribute><name>agent.thread.num</name><value>"+str(agent_threads)+"</value></attribute>\n"
    content.append(line)

    line="<attribute><name>cmddist.thread.num</name><value>"+str(cmddist_threads)+"</value></attribute>\n"
    content.append(line)

    line="<attribute><name>local.addr</name><value>"+node+"</value></attribute>\n"
    content.append(line)

    line="<attribute><name>block.directory</name><value>"+blk_dir+"</value></attribute>\n"
    content.append(line)

    line="<attribute><name>stripestore.directory</name><value>"+stripeStore_dir+"</value></attribute>\n"
    content.append(line)

    line="<attribute><name>tradeoffpoint.directory</name><value>"+tradeoffPoint_dir+"</value></attribute>\n"
    content.append(line)

    line="<attribute><name>eccluster.size</name><value>"+ECCSIZE+"</value></attribute>\n"
    content.append(line)

    line="<attribute><name>repair.thread.num</name><value>"+RPTHREADS+"</value></attribute>\n"
    content.append(line)

    line="</setting>\n"
    content.append(line)

    f=open("sysSetting.xml","w")
    for line in content:
        f.write(line)
    f.close()

    cmd="scp sysSetting.xml {}:{}".format(node, config_dir)
    os.system(cmd)

    cmd="rm sysSetting.xml"
    os.system(cmd)
