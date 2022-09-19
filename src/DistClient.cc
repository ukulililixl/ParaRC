//#include "common/CoorBench.hh"
#include "common/Config.hh"
#include "protocol/CoorCommand.hh"

#include "inc/include.hh"
#include "util/RedisUtil.hh"
#include "util/DistUtil.hh"

using namespace std;

void usage() {
  cout << "       ./DistClient degradeRead blockname method" << endl;
  cout << "       ./DistClient standbyRepair ip code method" << endl;
}

void degradeRead(string blockname, string method) {

    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);

    string confpath("./conf/sysSetting.xml");
    Config* conf = new Config(confpath);
    
    CoorCommand* cmd = new CoorCommand();
    cmd->buildType4(4, conf->_localIp, blockname, method);
    cmd->sendTo(conf->_coorIp);
    
    delete cmd;

    // wait for finish flag?
    redisContext* waitCtx = RedisUtil::createContext(conf->_localIp);
    string wkey = "writefinish:"+blockname;
    redisReply* fReply = (redisReply*)redisCommand(waitCtx, "blpop %s 0", wkey.c_str());
    freeReplyObject(fReply);
    redisFree(waitCtx);

    gettimeofday(&time2, NULL);
    cout << "repairBlock::repair time: " << DistUtil::duration(time1, time2) << endl;

    delete conf;
}

void standbyRepair(string nodeipstr, string code, string method) {

  string confpath("./conf/sysSetting.xml");
  Config* conf = new Config(confpath);

  unsigned int ip = inet_addr(nodeipstr.c_str());

  CoorCommand* cmd = new CoorCommand();
  cmd->buildType3(3, conf->_localIp, ip, code, method);
  cmd->sendTo(conf->_coorIp);

  delete cmd;
  delete conf;
}

int main(int argc, char** argv) {
    
    if (argc < 2) {
        usage();
        return -1;
    }
    
    string reqType(argv[1]);
    if (reqType == "degradeRead") {
        
        if (argc != 4) {
            usage();
            return -1;
        }
        
        string blockname(argv[2]);
        string method(argv[3]);
        degradeRead(blockname, method);
    } else if (reqType == "standbyRepair") {
        if (argc != 5) {
            usage();
            return -1;
        }

        string nodeip(argv[2]);
        string code(argv[3]);
        string method(argv[4]);
        standbyRepair(nodeip, code, method);
    }

  return 0;
}
