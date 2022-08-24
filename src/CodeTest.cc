#include "common/Config.hh"
#include "common/StripeMeta.hh"
#include "ec/ECDAG.hh"
#include "common/StripeStore.hh"
#include "protocol/AGCommand.hh"
#include "util/DistUtil.hh"

#include "ec/Clay.hh"
#include "ec/MISER.hh"
#include "ec/BUTTERFLY.hh"

#include "dist/Solution.hh"

using namespace std;

void usage() {
  cout << "Usage: ./TradeoffAnalysis" << endl;
  cout << "    1. code" << endl;
  cout << "    2. n" << endl;
  cout << "    3. k" << endl;
  cout << "    4. w" << endl;
  cout << "    5. pkt KiB" << endl;
  cout << "    6. block MiB" << endl;
  cout << "    7. repair index" << endl;
}  

double getCurrentTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (double)tv.tv_sec * 1e+6 + (double)tv.tv_usec;
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
  int pktKiB = atoi(argv[5]);
  int pktbytes = pktKiB * 1024;
  int blkMiB = atoi(argv[6]);
  int blkbytes = blkMiB * 1048576;
  int repairIdx = atoi(argv[7]);

  ECBase* ec;
  ECBase* dec;
  vector<string> param;
  if (code == "Clay") {
    ec = new Clay(n, k, w, {to_string(n-1)});
    dec = new Clay(n, k, w, {to_string(n-1)});
  } else if (code == "MISER") {
    ec = new MISER(n, k, w, param);
    dec = new MISER(n, k, w, param);
  } else if (code == "Butterfly") {
    ec = new BUTTERFLY(n, k, w, param);
    dec = new BUTTERFLY(n, k, w, param);
  } else {
    cout << "wrong ec id!" << endl;
    return -1;
  }
  w = ec->_w;
  cout << "w: " << w << endl;

  int stripenum = blkbytes / pktbytes;
  int slicebytes = pktbytes / w;

  string stripename = "stripe0";
  vector<string> blklist;
  for (int i=0; i<n; i++) {
      string blkname = "blk"+to_string(i);
      blklist.push_back(blkname);
  }

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
  char* repairbuffer = (char*)calloc(pktbytes, sizeof(char));

  // 1. prepare for encode
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

  // 2. prepare for decode
  ECDAG* decdag = nullptr;
  vector<ECTask*> decodetasks;
  unordered_map<int, char*> decodeBufMap;
  // 2.1 create decode tasks
  vector<int> availIdx;
  vector<int> toRepairIdx;
  for (int i=0; i<n; i++) {
      for (int j=0; j<w; j++) {
          int idx = i*w+j;
          if (i == repairIdx) {
              toRepairIdx.push_back(idx);
              char* buf = repairbuffer+j*slicebytes;
              decodeBufMap.insert(make_pair(idx, buf));
          } else {
              availIdx.push_back(idx);
              char* buf = buffers[i]+j*slicebytes;
              decodeBufMap.insert(make_pair(idx, buf));
          }
      }
  }
  decdag = dec->Decode(availIdx, toRepairIdx);
  decdag->Concact(toRepairIdx);
  decdag->genECUnits();

  // 2.2 generate ComputeTask from ECUnits
  vector<ComputeTask*> decctasklist;
  decdag->genComputeTaskByECUnits(decctasklist);

  // 2.3 put shortened pkts into encodeBufMap
  vector<int> decHeaders = decdag->getECLeaves();
  for (auto cid: decHeaders) {
      if (decodeBufMap.find(cid) == decodeBufMap.end()) {
        char* tmpbuf = (char*)calloc(pktbytes/w, sizeof(char));
        decodeBufMap.insert(make_pair(cid, tmpbuf));
      }
  }

  // 2. test
  double encodeTime = 0, decodeTime = 0;
  srand((unsigned)1234);
  cout << "stripenum: " << stripenum << endl;
  for (int stripei=0; stripei<stripenum; stripei++) {
    // clean codebuffers
    for (int i=k;i<n; i++) {
        memset(buffers[i], 0, pktbytes);
    }
    
    // initialize databuffers
    for (int i=0; i<k; i++) {
        for (int j=0; j<pktbytes; j++) {
            buffers[i][j] = rand();
        }
    }
    
    //// debug
    //for (int i=0; i<k; i++) {
    //    for (int j=0; j<w; j++) {
    //        char c = i*w+j;
    //        memset(buffers[i]+j*slicebytes, c, slicebytes);
    //    }
    //}

    // encode test
    encodeTime -= getCurrentTime();
    for (int taskid=0; taskid<encctasklist.size(); taskid++) {
        ComputeTask* cptask = encctasklist[taskid];
        vector<int> srclist = cptask->_srclist;
        vector<int> dstlist = cptask->_dstlist;
        vector<vector<int>> coefs = cptask->_coefs;

        //cout << "srclist: ";
        //for (int j=0; j<srclist.size(); j++)
        //  cout << srclist[j] << " ";
        //cout << endl;
        //for (int j=0; j<dstlist.size(); j++) {
        //  int target = dstlist[j];
        //  vector<int> coef = coefs[j];
        //  cout << "target: " << target << "; coef: ";
        //  for (int ci=0; ci<coef.size(); ci++) {
        //    cout << coef[ci] << " ";
        //  }
        //  cout << endl;
        //}

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

        // cout << "srclist: ";
        // for (int j=0; j<srclist.size(); j++)
        //   cout << srclist[j] << " ";
        // cout << endl;
        // for (int j=0; j<dstlist.size(); j++) {
        //   int target = dstlist[j];
        //   vector<int> coef = coefs[j];
        //   cout << "target: " << target << ", value: " << (int)code[j][0] << "; coef: ";
        //   for (int ci=0; ci<coef.size(); ci++) {
        //     cout << coef[ci] << " ";
        //   }
        //   cout << endl;
        // }
        // free(matrix);
        // free(data);
        // free(code);
    }

    encodeTime += getCurrentTime();

    // // debug encode
    // cout << "after encoding:" << endl;
    // for (int i=0; i<n; i++) {
    //     cout << "block[" << i << "]: ";
    //     for (int j=0; j<w; j++) {
    //         char c = buffers[i][j*slicebytes];
    //         cout << (int)c << " ";
    //     }
    //     cout << endl;
    // }

    // // debug decode
    // cout << "before decoding:" << endl;
    // for (int i=0; i<n; i++) {
    //     cout << "block[" << i << "]: ";
    //     for (int j=0; j<w; j++) {
    //         int idx = i*w+j;
    //         char* buf = decodeBufMap[idx];
    //         char c = buf[0];
    //         cout << (int)c << " ";
    //     }
    //     cout << endl;
    // }

    // decode test
    decodeTime -= getCurrentTime();
    for (int taskid=0; taskid<decctasklist.size(); taskid++) {
        ComputeTask* cptask = decctasklist[taskid];
        vector<int> srclist = cptask->_srclist;
        vector<int> dstlist = cptask->_dstlist;
        vector<vector<int>> coefs = cptask->_coefs;

        // now we create buf in bufMap
        for (auto dstidx: dstlist) {
            if (decodeBufMap.find(dstidx) == decodeBufMap.end()) {
                char* tmpbuf = (char*)calloc(slicebytes, sizeof(char));
                memset(tmpbuf, 0, slicebytes);
                decodeBufMap.insert(make_pair(dstidx, tmpbuf));
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
            assert (decodeBufMap.find(child) != decodeBufMap.end());
            data[bufIdx] = decodeBufMap[child];
            unsigned char c = data[bufIdx][0];
        }
        // prepare code buf and matrix
        for (int codeBufIdx = 0; codeBufIdx < dstlist.size(); codeBufIdx++) {
            int target = dstlist[codeBufIdx];
            char* codebuf;
            assert(decodeBufMap.find(target) != decodeBufMap.end());
            code[codeBufIdx] = decodeBufMap[target];
            vector<int> curcoef = coefs[codeBufIdx];
            for (int j=0; j<col; j++) {
                matrix[codeBufIdx * col + j] = curcoef[j];
            }
        }
        // perform computation
        Computation::Multi(code, data, matrix, row, col, slicebytes, "Isal");

        // cout << "srclist: ";
        // for (int j=0; j<srclist.size(); j++)
        //   cout << srclist[j] << " ";
        // cout << endl;
        // for (int j=0; j<dstlist.size(); j++) {
        //   int target = dstlist[j];
        //   vector<int> coef = coefs[j];
        //   cout << "target: " << target << ", value: " << (int)code[j][0] << "; coef: ";
        //   for (int ci=0; ci<coef.size(); ci++) {
        //     cout << coef[ci] << " ";
        //   }
        //   cout << endl;
        // }

        free(matrix);
        free(data);
        free(code);
    }
    decodeTime += getCurrentTime();

    // check correcness
    bool success = true;
    for (int i=0; i<pktbytes; i++) {
        if (buffers[repairIdx][i] != repairbuffer[i]){
            success = false;
            cout << "repair error at offset: " << i << endl;
            break;
        }
    }

    if (!success) {
        cout << "repair error!" << endl;
        break;
    }
  }

  cout << "Encode Time: " << encodeTime/1000 << " ms" << endl;
  cout << "Encode Trpt: " << blkbytes * k /1.048576/encodeTime << endl;
  cout << "Decode Time: " << decodeTime/1000 << " ms" << endl;
  cout << "Decode Trpt: " << blkbytes / 1.048576/encodeTime << endl;


  return 0;
}
