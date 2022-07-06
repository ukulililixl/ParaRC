//#include "common/CoorBench.hh"
#include "common/Config.hh"
#include "protocol/CoorCommand.hh"

#include "inc/include.hh"
#include "util/RedisUtil.hh"
using namespace std;

void usage() {
  cout << "usage: ./DistClient repairBlock blockname method" << endl;
  cout << "       ./DistClient repairNode ip code method" << endl;
}

void repairBlock(string blockname, string method) {

  string confpath("./conf/sysSetting.xml");
  Config* conf = new Config(confpath);

  CoorCommand* cmd = new CoorCommand();
  cmd->buildType0(0, conf->_localIp, blockname, method);
  cmd->sendTo(conf->_coorIp);

  delete cmd;
  delete conf;
}

void repairNode(string nodeipstr, string code, string method) {

  string confpath("./conf/sysSetting.xml");
  Config* conf = new Config(confpath);

  unsigned int ip = inet_addr(nodeipstr.c_str());

  CoorCommand* cmd = new CoorCommand();
  cmd->buildType1(1, conf->_localIp, ip, code, method);
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
    if (reqType == "repairBlock") {
        if (argc != 4) {
            usage();
            return -1;
        }
        
        string blockname(argv[2]);
        string method(argv[3]);
        repairBlock(blockname, method);
    } else if (reqType == "repairNode") {
        if (argc != 5) {
            usage();
            return -1;
        }

        string nodeip(argv[2]);
        string code(argv[3]);
        string method(argv[4]);
        repairNode(nodeip, code, method);
    }
//  if (reqType == "write") {
//    if (argc != 7) {
//      usage();
//      return -1;
//    }
//    string inputfile(argv[2]);
//    string filename(argv[3]);
//    string ecid(argv[4]);
//    string mode(argv[5]);
//    int size = atoi(argv[6]);
//    if ((mode == "online") || (mode == "offline")) {
//      write(inputfile, filename, ecid, mode, size)    ;
//    } else {
//      cout << "Error encodemode: Only support online/offline encode mode" << endl;
//      return -1;
//    }   
//  } else if (reqType == "read") {
//    if (argc != 4) {
//      usage();
//      return -1;
//    }
//    string filename(argv[2]);
//    string saveas(argv[3]);
//    read(filename, saveas);
//  } else if (reqType == "startEncode") {
//    string confpath("./conf/sysSetting.xml");
//    Config* conf = new Config(confpath);    
//    // send coorCmd to coordinator?
//    CoorCommand* cmd = new CoorCommand();
//    cmd->buildType7(7, 1, "encode");
//    cmd->sendTo(conf->_coorIp);
//    
//    delete cmd;
//    delete conf;
//  } else if (reqType == "startRepair") {
//    string confpath("./conf/sysSetting.xml");
//    Config* conf = new Config(confpath);
//    // send coorCmd to coordinator
//    CoorCommand* cmd = new CoorCommand();
//    cmd->buildType7(7, 1, "repair");
//    cmd->sendTo(conf->_coorIp);
//    delete cmd;
//    delete conf;
//  } else if (reqType == "coorBench") {
//    if (argc != 4) {
//      usage();
//      return -1;
//    }
//    int id = atoi(argv[2]);
//    int number = atoi(argv[3]);
//    string confpath("./conf/sysSetting.xml");
//    Config* conf = new Config(confpath);
//    CoorBench* benchClient = new CoorBench(conf, id, number);
//    benchClient->close();
//
//    delete benchClient;
//    delete conf;
//  } else if (reqType == "writeLayer") {
//      if (argc != 8) {
//          usage();
//          return -1;
//
//      }
//      string inputfile(argv[2]);
//      string filename(argv[3]);
//      string ecid(argv[4]);
//      string mode(argv[5]);
//      int size = atoi(argv[6]);
//      string layer(argv[7]);
//      if ((mode == "online") || (mode == "offline")) {
//          writeLayer(inputfile, filename, ecid, mode, size, layer);
//
//      } else {
//          cout << "Error encodemode: Only support online/offline encode mode" << endl;
//          return -1;
//
//      }   
//  } else {
//    cout << "ERROR: un-recognized request!" << endl;
//    usage();
//    return -1;
//  }

  return 0;
}
