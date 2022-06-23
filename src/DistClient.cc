//#include "common/CoorBench.hh"
#include "common/Config.hh"
#include "protocol/CoorCommand.hh"

#include "inc/include.hh"
#include "util/RedisUtil.hh"
using namespace std;

void usage() {
  cout << "usage: ./DistClient repairBlock blockname method" << endl;
//  cout << "usage: ./OECClient write inputfile saveas ecid online sizeinMB" << endl;
//  cout << "       ./OECClient write inputfile saveas poolid offline sizeinMB" << endl;
//  cout << "       ./OECClient read filename saveas" << endl;
//  cout << "       ./OECClient startEncode" << endl;
//  cout << "       ./OECClient startRepair" << endl;
//  cout << "       ./OECClient coorBench id number" << endl;
//  cout << "       ./OECClient writeLayer inputfile saveas ecid online sizeinMB layer" << endl;
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
// 
//void write(string inputname, string filename, string ecidpool, string encodemode, int sizeinMB) {
//   string confpath("./conf/sysSetting.xml");
//   Config* conf = new Config(confpath);
//   struct timeval time1, time2, time3, time4;
//   gettimeofday(&time1, NULL);
// 
//   // 0. create input file
//   FILE* inputfile = fopen(inputname.c_str(), "rb");
// 
//   // 1. create OECOutputStream and init
//   OECOutputStream* outstream = new OECOutputStream(conf, filename, ecidpool, encodemode, sizeinMB);
//   gettimeofday(&time2, NULL);
// 
//   int sizeinBytes = sizeinMB * 1048576;
//   int num = sizeinBytes/conf->_pktSize;
//   cout << "num = " << num << endl;
//   srand((unsigned)time(0));
// 
//   for (int i=0; i<num; i++) {
//     char* buf = (char*)calloc(conf->_pktSize+4, sizeof(char));
//  
//     int tmplen = htonl(conf->_pktSize);
//     memcpy(buf, (char*)&tmplen, 4);
// 
//     fread(buf+4, conf->_pktSize, 1, inputfile);
//     outstream->write(buf, conf->_pktSize+4);
//     free(buf);
//   } 
// 
//   gettimeofday(&time3, NULL);
//   outstream->close();
//   gettimeofday(&time4, NULL);
// 
////   cout << "OECClient::create OECOutputStream: " << RedisUtil::duration(time1, time2)<< endl;
////   cout << "OECClient::write all data into redis: " << RedisUtil::duration(time2, time3) << endl;
////   cout << "OECClient::wait for ack: " << RedisUtil::duration(time3, time4) << endl;
//   cout << "OECClient::write.overall.duration: " << RedisUtil::duration(time1, time4) << endl;
//   struct timeval close1, close2;
//   gettimeofday(&close1, NULL);
//   fclose(inputfile);
//   gettimeofday(&close2, NULL);
////   cout << "OECClient::overall.write.close inputfile: " << RedisUtil::duration(close1, close2) << endl;
//   delete outstream;
//   delete conf;
//}
//
//void writeLayer(string inputname, string filename, string ecidpool, string encodemode, int sizeinMB, string layer) {
//	string confpath("./conf/sysSetting.xml");
//	Config* conf = new Config(confpath);
//	struct timeval time1, time2, time3, time4;
//	gettimeofday(&time1, NULL);
//
//	// 0. create input file
//	FILE* inputfile = fopen(inputname.c_str(), "rb");
//
//	// 1. create OECOutputStream and init
//	OECOutputStream* outstream = new OECOutputStream(conf, filename, ecidpool, encodemode, sizeinMB, layer);
//	gettimeofday(&time2, NULL);
//
//	int sizeinBytes = sizeinMB * 1048576;
//	int num = sizeinBytes/conf->_pktSize;
//	cout << "num = " << num << endl;
//	srand((unsigned)time(0));
//
//	for (int i=0; i<num; i++) {
//		char* buf = (char*)calloc(conf->_pktSize+4, sizeof(char));
//
//		int tmplen = htonl(conf->_pktSize);
//		memcpy(buf, (char*)&tmplen, 4);
//
//		fread(buf+4, conf->_pktSize, 1, inputfile);
//		outstream->write(buf, conf->_pktSize+4);
//		free(buf);
//	} 
//
//	gettimeofday(&time3, NULL);
//	outstream->close();
//	gettimeofday(&time4, NULL);
//
//	//   cout << "OECClient::create OECOutputStream: " << RedisUtil::duration(time1, time2)<< endl;
//	//   cout << "OECClient::write all data into redis: " << RedisUtil::duration(time2, time3) << endl;
//	//   cout << "OECClient::wait for ack: " << RedisUtil::duration(time3, time4) << endl;
//	cout << "OECClient::write.overall.duration: " << RedisUtil::duration(time1, time4) << endl;
//	struct timeval close1, close2;
//	gettimeofday(&close1, NULL);
//	fclose(inputfile);
//	gettimeofday(&close2, NULL);
//	//   cout << "OECClient::overall.write.close inputfile: " << RedisUtil::duration(close1, close2) << endl;
//	delete outstream;
//	delete conf;
//}

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
