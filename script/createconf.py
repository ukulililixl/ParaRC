# usage:
#   python createconf.py
# 	1.blockbytes [1048576]
# 	2.packetbytes [1048576]

import os
import sys
import subprocess

BLKBYTES=sys.argv[1]
PKTBYTES=sys.argv[2]

cluster=[
"192.168.3.123",
"192.168.3.124",
"192.168.3.133",
"192.168.3.137",
"192.168.3.138",
"192.168.3.140",
]

node2rack={
"192.168.3.123":"/rack0",
"192.168.3.124":"/rack0",
"192.168.3.133":"/rack0",
"192.168.3.137":"/rack0",
"192.168.3.138":"/rack0",
"192.168.3.140":"/rack0",
}

for node in cluster:

    content=[]

    line="<setting>\n"
    content.append(line)

    line="<attribute><name>controller.addr</name><value>192.168.3.123</value></attribute>\n"
    content.append(line)

    line="<attribute><name>agents.addr</name>\n"
    content.append(line)

    for agent in cluster:
        if agent == "192.168.3.123":
            continue
        rack = node2rack[agent]
        line="<value>/default/"+agent+"</value>\n"
        #line="<value>"+rack+"/"+agent+"</value>\n"
        content.append(line)
        
    line="</attribute>\n"
    content.append(line)

    line="<attribute><name>controller.thread.num</name><value>1</value></attribute>\n"
    content.append(line)
    
    #line="<attribute><name>oec.agent.thread.num</name><value>20</value></attribute>\n"
    line="<attribute><name>agent.thread.num</name><value>5</value></attribute>\n"
    content.append(line)
    
    line="<attribute><name>cmddist.thread.num</name><value>10</value></attribute>\n"
    content.append(line)
    
    line="<attribute><name>local.addr</name><value>"+node+"</value></attribute>\n"
    content.append(line)

    line="<attribute><name>block.size</name><value>"+BLKBYTES+"</value></attribute>\n"
    content.append(line)
    
    line="<attribute><name>packet.size</name><value>"+PKTBYTES+"</value></attribute>\n"
    content.append(line)
    
    #line="<attribute><name>ec.concurrent.num</name><value>15</value></attribute>\n"
    #content.append(line)

    fspath="/home/xiaolu/HDFS/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/dfs/data/current/BP-213413475-192.168.3.123-1649731498928/current/finalized/subdir0/subdir0"
    line="<attribute><name>block.directory</name><value>"+fspath+"</value></attribute>"
    content.append(line)

    sspath="/home/xiaolu/spdist/SPDist/stripeStore"
    line="<attribute><name>stripestore.directory</name><value>"+sspath+"</value></attribute>"
    content.append(line)
    
    line="</setting>\n"
    content.append(line)
    
    f=open("sysSetting.xml","w")
    for line in content:
        f.write(line)
    f.close()
    
    cmd="scp sysSetting.xml "+node+":/home/xiaolu/spdist/SPDist/conf"
    os.system(cmd)
