#include "common/Config.hh"
#include "common/StripeMeta.hh"
#include "ec/ECDAG.hh"
#include "common/StripeStore.hh"
#include "protocol/AGCommand.hh"

#include "ec/RSCONV.hh"
#include "ec/RSPIPE.hh"

using namespace std;

void usage() {
  cout << "Usage: ./GenData" << endl;
  cout << "    1. code" << endl;
  cout << "    2. n" << endl;
  cout << "    3. k" << endl;
  cout << "    4. w" << endl;
  cout << "    5. stripeidx" << endl;
  cout << "    6. blockbytes" << endl;
  cout << "    7. pktbytes" << endl;
}  

double getCurrentTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (double)tv.tv_sec * 1e+6 + (double)tv.tv_usec;
}

vector<string> genMetaData(string code, int n, int k, int w, string stripename, vector<string> blklist, int blockbytes, int pktbytes) {
  vector<string> toret;
  string line;
  line = "<stripe>\n";
  toret.push_back(line);

  line = "<attribute><name>code</name><value>"+code+"</value></attribute>\n";
  toret.push_back(line);

  line = "<attribute><name>ecn</name><value>"+to_string(n)+"</value></attribute>\n";
  toret.push_back(line);

  line = "<attribute><name>eck</name><value>"+to_string(k)+"</value></attribute>\n";
  toret.push_back(line);

  line = "<attribute><name>ecw</name><value>"+to_string(w)+"</value></attribute>\n";
  toret.push_back(line);

  line = "<attribute><name>stripename</name><value>"+stripename+"</value></attribute>\n";
  toret.push_back(line);

  line = "<attribute><name>blocklist</name>\n";
  toret.push_back(line);
  for (auto item: blklist) {
    line = "<value>"+item+":0.0.0.0</value>\n";
    toret.push_back(line);
  }
  line = "</attribute>\n";
  toret.push_back(line);

  line = "<attribute><name>blockbytes</name><value>"+to_string(blockbytes)+"</value></attribute>\n";
  toret.push_back(line);

  line = "<attribute><name>pktbytes</name><value>"+to_string(pktbytes)+"</value></attribute>\n";
  toret.push_back(line);

  line = "</stripe>\n";
  toret.push_back(line);

  return toret;
}

int main(int argc, char** argv) {

  if (argc != 8) {
    usage();
    return 0;
  }

  string code = argv[1];
  int n = atoi(argv[2]);
  int k = atoi(argv[3]);
  int w = atoi(argv[4]);
  int stripeidx = atoi(argv[5]);
  int blkbytes = atoi(argv[6]);
  int pktbytes = atoi(argv[7]);
  string stripename = "stripe-"+to_string(stripeidx);

  string configpath="./conf/sysSetting.xml";
  string blkdir = "./blkDir";
  Config* conf = new Config(configpath);
  vector<string> blklist;
  for (int i=0; i<n; i++) {
    string blkname = stripename + "-" + to_string(i);
    blklist.push_back(blkname);
  }

  ECBase* ec;
  if (code == "Clay") {
      ec = new Clay(n,k,w,{to_string(n-1)});
  } else if (code == "RSCONV") {
      ec = new RSCONV(n,k,w,{});
  } else if (code == "RSPIPE") {
      ec = new RSPIPE(n,k,w,{});
  }
  w = ec->_w;

  int stripenum = blkbytes / pktbytes;
  int slicebytes = pktbytes / w;
  int pktnum = blkbytes/pktbytes;

  // 0. prepare buffers
  char** buffers = (char**)calloc(n, sizeof(char*));
  for (int i=0; i<n; i++) {
      buffers[i] = (char*)calloc(pktbytes, sizeof(char));
      memset(buffers[i], 0, pktbytes);
      if (i < k) { 
          for (int j=0; j<w; j++) {
              int cid = i*w+j;
              memset(buffers[i], cid, pktbytes);
          }
      }
  }

  // 1. encode ecdag
  ECDAG* encdag = nullptr;
  vector<ECTask*> encodetasks;
  unordered_map<int, char*> encodeBufMap;
  // 1.1 create encode tasks
  encdag = ec->Encode();
  encdag->genECUnits();
  // 1.2 generate ComputeTask from ECUnits
  vector<ComputeTask*> encctasklist; 
  encdag->genComputeTaskByECUnits(encctasklist);
  // 1.3 put slices into encodeBufMap
  for (int i=0; i<n; i++) {
      for (int j=0; j<w; j++) {
          int idx = i*w+j;
          char* buf = buffers[i]+j*slicebytes;
          encodeBufMap.insert(make_pair(idx, buf));
      }
  }
  // 1.4 put shortened pkts into encodeBufMap
  vector<int> encHeaders = encdag->getECLeaves();
  for (auto cid: encHeaders) {
      if (encodeBufMap.find(cid) == encodeBufMap.end()) {
          char* tmpbuf = (char*)calloc(pktbytes/w, sizeof(char));
          encodeBufMap.insert(make_pair(cid, tmpbuf));
      }
  }

  for (int taskid=0; taskid<encctasklist.size(); taskid++) {
      ComputeTask* cptask = encctasklist[taskid];
      vector<int> srclist = cptask->_srclist;
      vector<int> dstlist = cptask->_dstlist;
      vector<vector<int>> coefs = cptask->_coefs;

      // now we create buf in bufMap
      for (auto dstidx: dstlist) {
          if (encodeBufMap.find(dstidx) == encodeBufMap.end()) {
              char* tmpbuf = (char*)calloc(slicebytes, sizeof(char));
              memset(tmpbuf, 0, slicebytes);
              encodeBufMap.insert(make_pair(dstidx, tmpbuf));
          }
      }

      int col = srclist.size();
      int row = dstlist.size();
      int* matrix = (int*)calloc(row*col, sizeof(int)); 
      char** data = (char**)calloc(col, sizeof(char*));
      char** code = (char**)calloc(row, sizeof(char*));
      // prepare data buf
      for (int bufIdx = 0; bufIdx < srclist.size(); bufIdx++) {
          int child = srclist[bufIdx];
          // check whether there is buf in databuf 
          assert (encodeBufMap.find(child) != encodeBufMap.end()); 
          data[bufIdx] = encodeBufMap[child];
          unsigned char c = data[bufIdx][0];
      }
      // prepare code buf and matrix
      for (int codeBufIdx = 0; codeBufIdx < dstlist.size(); codeBufIdx++) {
          int target = dstlist[codeBufIdx];
          char* codebuf;
          assert(encodeBufMap.find(target) != encodeBufMap.end());
          code[codeBufIdx] = encodeBufMap[target];
          vector<int> curcoef = coefs[codeBufIdx];
          for (int j=0; j<col; j++) {
              matrix[codeBufIdx * col + j] = curcoef[j];
          }
      }
      // perform computation
      Computation::Multi(code, data, matrix, row, col, slicebytes, "Isal");
  }

  // write databuffer to blkDir
  for (int i=0; i<n; i++) {
    string blkname = blklist[i];
    string fullpath = blkdir + "/" + blkname;
    ofstream ofs(fullpath);
    ofs.close();
    ofs.open(fullpath, ios::app);
    for (int j=0; j<pktnum; j++)
        ofs.write(buffers[i], pktbytes);
    free(buffers[i]);
    ofs.close();
  }  

  // // write metadata
  // vector<string> metadata = genMetaData(code, n, k, w, stripename, blklist, blkbytes, pktbytes);
  // string metapath = "./stripeStore/"+stripename+".xml";
  // ofstream metaofs(metapath, ofstream::app);
  // for (auto line: metadata) {
  //   metaofs << line;
  // }
  // metaofs.close();

  for (auto t: encctasklist)
    delete t;
  delete encdag;
  delete ec;
  delete conf;
  
  return 0;
}
