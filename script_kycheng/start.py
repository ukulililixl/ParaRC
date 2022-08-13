import os
import subprocess

# home dir
cmd = r'echo ~'
home_dir_str, stderr = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
home_dir = home_dir_str.decode().strip()
# proj dir
proj_dir="{}/SPDist".format(home_dir)
config_dir="{}/conf".format(proj_dir)
CONF = config_dir+"/sysSetting.xml"
script_dir = "{}/script_kycheng".format(proj_dir)
stripestore_dir="{}/stripeStore".format(proj_dir)


f=open(CONF)
start = False
concactstr = ""
for line in f:
    if line.find("setting") == -1:
        line = line[:-1]
    concactstr += line
res=concactstr.split("<attribute>")

slavelist=[]
fstype=""
for attr in res:
    if attr.find("dss.type") != -1:
        attrtmp=attr.split("<value>")[1]
        fstype=attrtmp.split("</value>")[0]
    if attr.find("agents.addr") != -1:
        valuestart=attr.find("<value>")
        valueend=attr.find("</attribute>")
        attrtmp=attr[valuestart:valueend]
        slavestmp=attrtmp.split("<value>")
        for slaveentry in slavestmp:
            if slaveentry.find("</value>") != -1:
                entrysplit=slaveentry.split("/")
                slave=entrysplit[2][0:-1]
                slavelist.append(slave)
    if attr.find("clients.addr") != -1:
        valuestart=attr.find("<value>")
        valueend=attr.find("</attribute>")
        attrtmp=attr[valuestart:valueend]
        slavestmp=attrtmp.split("<value>")
        for slaveentry in slavestmp:
            if slaveentry.find("</value>") != -1:
                entrysplit=slaveentry.split("/")
                slave=entrysplit[2][0:-1]
                slavelist.append(slave)

# start
print("start coordinator")
os.system("redis-cli flushall")
os.system("killall DistCoordinator")
os.system("sudo service redis_6379 restart")

# create stripestore dir
command="mkdir -p " + stripestore_dir
subprocess.Popen(['/bin/bash', '-c', command])

# open controller
command="cd "+proj_dir+"; ./DistCoordinator &> "+proj_dir+"/coor_output &"
subprocess.Popen(['/bin/bash', '-c', command])

for slave in slavelist:
    print("start slave on ", slave)
    command="sleep 1"
    os.system(command)

    os.system("ssh " + slave + " \"killall DistAgent \"")
    os.system("ssh " + slave + " \"killall DistClient \"")
    os.system("ssh " + slave + " \"sudo service redis_6379 restart\"")
    command="scp "+proj_dir+"/DistAgent "+slave+":"+proj_dir+"/"
    os.system(command)
    command="scp "+proj_dir+"/DistClient "+slave+":"+proj_dir+"/"
    os.system(command)
    os.system("ssh " + slave + " \"redis-cli flushall \"")
    command="ssh "+slave+" \"cd "+proj_dir+"; ./DistAgent &> "+proj_dir+"/agent_output &\""
    os.system(command)

    
