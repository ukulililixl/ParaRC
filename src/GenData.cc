#include "common/Config.hh"
#include "common/StripeMeta.hh"
#include "ec/ECDAG.hh"
#include "common/StripeStore.hh"
#include "protocol/AGCommand.hh"

using namespace std;

void usage() {
  cout << "Usage: ./GenData" << endl;
  cout << "    1. code" << endl;
  cout << "    2. n" << endl;
  cout << "    3. k" << endl;
  cout << "    4. w" << endl;
  cout << "    5. stripeidx" << endl;
}  

double getCurrentTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (double)tv.tv_sec * 1e+6 + (double)tv.tv_usec;
}

vector<string> genMetaData(string code, int n, int k, int w, string stripename, vector<string> blklist) {
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

  line = "</stripe>\n";
  toret.push_back(line);

  return toret;
}

int main(int argc, char** argv) {

  if (argc != 6) {
    usage();
    return 0;
  }

  string code = argv[1];
  int n = atoi(argv[2]);
  int k = atoi(argv[3]);
  int w = atoi(argv[4]);
  int stripeidx = atoi(argv[5]);
  string stripename = "stripe-"+to_string(stripeidx);

  string configpath="./conf/sysSetting.xml";
  string blkdir = "./blkDir";
  Config* conf = new Config(configpath);
  vector<string> blklist;
  for (int i=0; i<n; i++) {
    string blkname = stripename + "-" + to_string(i);
    blklist.push_back(blkname);
  }

  int blksizeB = conf->_blkSize;
  int pktsizeB = conf->_pktSize;

  ECBase* ec;
  if (code == "Clay")
    ec = new Clay(n, k, w, {to_string(n-1)});
  w = ec->_w;

  if (pktsizeB % w != 0) {
    pktsizeB = pktsizeB / w * w;
    blksizeB = pktsizeB;
    cout << "pktsizeB: " << pktsizeB << endl;
  }

  int subpktsize = pktsizeB / w;
  // prepare original blocks
  char** buffers = (char**)calloc(n, sizeof(char*));
  unordered_map<int, char*> bufMap;
  vector<int> parityidx;
  for (int i=0; i<n; i++) {
    buffers[i] = (char*)calloc(pktsizeB, sizeof(char));
    if (i < k) {
      for (int j=0; j<w; j++) {
        char idx = i*w+j;
        memset(buffers[i]+j*subpktsize, idx, subpktsize);
      }
    } else {
      memset(buffers[i], 0, pktsizeB);
      for (int j=0; j<w; j++) {
        int idx = i*w+j;
        parityidx.push_back(idx);
      }
    }
    
    for (int j=0; j<w; j++) {
      bufMap.insert(make_pair(i*w+j, buffers[i]+j*subpktsize));
    }
  }

  // debug data
  for (int i=0; i<k; i++) {
    for (int j=0; j<w; j++) {
      int idx = i*w+j;
      char c = bufMap[idx][0];
      cout << "idx: " << idx << ", value: " << (int)c << endl;
    }
  }

  ECDAG* ecdag = ec->Encode();
  ecdag->Concact(parityidx);
  ecdag->genSimpleGraph();
  ecdag->simulateLoc();

  vector<ECTask*> tasklist;
  ecdag->genECTasks(tasklist, n, k, w, stripename, blklist);

  for (int taskid=0; taskid<tasklist.size(); taskid++) {
    ECTask* curtask = tasklist[taskid];
    int type = curtask->getType();
    if (type != 1)
      continue;
    cout << "===" << endl;
    //cout << curtask->dumpStr() << endl;
    vector<ComputeTask*> ctlist = curtask->getComputeTaskList();
    for (int i=0; i<ctlist.size(); i++) {
      ComputeTask* cptask = ctlist[i];
      vector<int> srclist = cptask->_srclist;
      vector<int> dstlist = cptask->_dstlist;
      vector<vector<int>> coefs = cptask->_coefs;
      cout << "srclist: ";
      for (int j=0; j<srclist.size(); j++)
        cout << srclist[j] << " ";
      cout << endl;
      for (int j=0; j<dstlist.size(); j++) {
        int target = dstlist[j];
        vector<int> coef = coefs[j];
        cout << "target: " << target << "; coef: ";
        for (int ci=0; ci<coef.size(); ci++) {
          cout << coef[ci] << " ";
        } 
        cout << endl;
      }

      // make sure that index in srclits has been in bufmap
      for(auto srcidx: srclist)
        assert(bufMap.find(srcidx) != bufMap.end());
      // now we create buf in bufMap
      for (auto dstidx: dstlist) {
        if (bufMap.find(dstidx) == bufMap.end()) {
          char* tmpbuf = (char*)calloc(pktsizeB/w, sizeof(char));
          memset(tmpbuf, 0, pktsizeB/w);
          bufMap.insert(make_pair(dstidx, tmpbuf));
        }
      }

      int col = srclist.size();
      int row = dstlist.size();
      int* matrix = (int*)calloc(row*col, sizeof(int));
      char** data = (char**)calloc(col, sizeof(char*));
      char** code = (char**)calloc(row, sizeof(char*));
      // prepare data buf
      cout << "---" << endl;
      for (int bufIdx = 0; bufIdx < srclist.size(); bufIdx++) {
        int child = srclist[bufIdx];
        // check whether there is buf in databuf
        assert (bufMap.find(child) != bufMap.end());
        data[bufIdx] = bufMap[child];
        unsigned char c = data[bufIdx][0];
        cout << "dataidx: " << child << ", value: " << (int)c << endl; 
      }
      // prepare code buf and matrix
      for (int codeBufIdx = 0; codeBufIdx < dstlist.size(); codeBufIdx++) {
        int target = dstlist[codeBufIdx];
        char* codebuf;
        assert(bufMap.find(target) != bufMap.end());
        code[codeBufIdx] = bufMap[target];
        vector<int> curcoef = coefs[codeBufIdx];
        for (int j=0; j<col; j++) {
          matrix[codeBufIdx * col + j] = curcoef[j];
        }
      }
      // perform computation
      Computation::Multi(code, data, matrix, row, col, pktsizeB/w, "Isal");
      for (int codeBufIdx = 0; codeBufIdx < dstlist.size(); codeBufIdx++) {
        int target = dstlist[codeBufIdx];
        unsigned char c = code[codeBufIdx][0];
        cout << "codeidx: " << target << ", value: " << (int)c << endl;
      } 
      
    }    
  }

  // write databuffer to blkDir
  for (int i=0; i<n; i++) {
    string blkname = blklist[i];
    string fullpath = blkdir + "/" + blkname;
    ofstream ofs(fullpath);
    ofs.close();
    ofs.open(fullpath, ios::app);
    ofs.write(buffers[i], pktsizeB);
    free(buffers[i]);
    ofs.close();
  }  

  // write metadata
  vector<string> metadata = genMetaData(code, n, k, w, stripename, blklist);
  string metapath = "./stripeStore/"+stripename+".xml";
  ofstream metaofs(metapath, ofstream::app);
  for (auto line: metadata) {
    metaofs << line;
  }
  metaofs.close();

  for (auto t: tasklist)
    delete t;
  delete ecdag;
  delete ec;
  delete conf;
  
  return 0;
}
