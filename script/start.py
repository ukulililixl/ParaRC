import os
import subprocess

filepath=os.path.realpath(__file__)
script_dir = os.path.dirname(os.path.normpath(filepath))
home_dir = os.path.dirname(os.path.normpath(script_dir))
conf_dir = home_dir+"/conf"
CONF = conf_dir+"/sysSetting.xml"

f = open(CONF)
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
# start
print "start coordinator"
os.system("redis-cli flushall")
os.system("killall DistCoordinator")
os.system("sudo service redis_6379 restart")
command="cd "+home_dir+"; ./DistCoordinator &> "+home_dir+"/coor_output &"

subprocess.Popen(['/bin/bash', '-c', command])

for slave in slavelist:
    print "start slave on " + slave
    os.system("ssh " + slave + " \"killall DistAgent \"")
    os.system("ssh " + slave + " \"killall DistClient \"")
    os.system("ssh " + slave + " \"sudo service redis_6379 restart\"")
    command="scp "+home_dir+"/DistAgent "+slave+":"+home_dir+"/"
    os.system(command)
    command="scp "+home_dir+"/DistClient "+slave+":"+home_dir+"/"
    os.system(command)
    os.system("ssh " + slave + " \"redis-cli flushall \"")
    command="ssh "+slave+" \"cd "+home_dir+"; ./DistAgent &> "+home_dir+"/agent_output &\""
    os.system(command)
