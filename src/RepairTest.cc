#include "common/Config.hh"
#include "common/StripeMeta.hh"
#include "ec/ECDAG.hh"
#include "common/StripeStore.hh"
#include "protocol/AGCommand.hh"

using namespace std;

void usage() {
  cout << "Usage: ./RepairTest" << endl;
  cout << "    1. stripename" << endl;
  cout << "    2. repairIdx" << endl;
}

double getCurrentTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (double)tv.tv_sec * 1e+6 + (double)tv.tv_usec;
}

int main(int argc, char** argv) {

  if (argc != 3) {
    usage();
    return 0;
  }

  string stripename = argv[1];
  int repairIdx = atoi(argv[2]);

  string configpath="./conf/sysSetting.xml";
  string blkdir = "./blkDir";
  Config* conf = new Config(configpath);
  StripeStore* ss = new StripeStore(conf);

  StripeMeta* stripeMeta = ss->getStripeMetaFromStripeName(stripename);
  assert(stripeMeta != NULL);
  int n = stripeMeta->getECN();
  int k = stripeMeta->getECK();
  int w = stripeMeta->getECW();
  vector<string> blklist = stripeMeta->getBlockList();

  int blksizeB = conf->_blkSize;
  int pktsizeB = conf->_pktSize;
  // 0. read original blocks
  char** buffers = (char**)calloc(n, sizeof(char*));
  for (int i=0; i<n; i++) {
    buffers[i] = (char*)calloc(pktsizeB, sizeof(char)); 

    string fullpath=blkdir+"/"+blklist[i];
    cout << "blk" << i << ": " << fullpath << endl;
    int fd = open(fullpath.c_str(), O_RDONLY);
    int readLen = 0, readl = 0;
    while (readLen < blksizeB) {
      if ((readl = pread(fd,
        buffers[i] + readLen,
        blksizeB - readLen,
        0)) < 0) {
        cout << "ERROR during disk read" << endl;
      } else {
        readLen += readl;
      }
    }
    close(fd);
  }
  // debug
  for (int i=0; i<n; i++) {
    for (int j=0; j<w; j++) {
      cout << "pktIdx " << i*w+j << ": " << (int)buffers[i][j*pktsizeB/w] << endl;
    }
  } 

  // 1. repair 
  char* repairBuf = (char*)calloc(pktsizeB, sizeof(char));
  vector<int> availIdx;
  vector<int> toRepairIdx;
  unordered_map<int, char*> bufMap;
  for (int i=0; i<n; i++) {
    for (int j=0; j<w; j++) {
      int pktidx = i*w+j;
      if (i == repairIdx) {
        toRepairIdx.push_back(pktidx);
        char* buf = repairBuf+j*pktsizeB/w;
        bufMap.insert(make_pair(pktidx, buf));
      }else {
        availIdx.push_back(pktidx);
        char* buf = buffers[i]+j*pktsizeB/w;
        bufMap.insert(make_pair(pktidx, buf));
      }
    }
  }

  ECBase* ec = stripeMeta->createECClass();
  ECDAG* ecdag = ec->Decode(availIdx, toRepairIdx);
  ecdag->Concact(toRepairIdx);
  ecdag->dump();

  ecdag->genSimpleGraph();
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

  delete conf;

  // compare
  bool success = true;
  for (int i=0; i<pktsizeB; i++) {
    if (repairBuf[i] != buffers[repairIdx][i]) {
      success = false;
      break;
    }
  }  
  if (success) 
    cout << "repair success!" << endl;
  else
    cout << "repair fail!" << endl;
  
  return 0;
}
