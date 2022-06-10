#include "Worker.hh"

Worker::Worker(Config* conf, int id) : _conf(conf) {
  _id = id;
  // create local context
  try {
    _processCtx = RedisUtil::createContext(_conf -> _localIp);
    _localCtx = RedisUtil::createContext(_conf -> _localIp);
    _coorCtx = RedisUtil::createContext(_conf -> _coorIp);
  } catch (int e) {
    // TODO: error handling
    cerr << "initializing redis context error" << endl;
  }
}

Worker::~Worker() {
  redisFree(_localCtx);
  redisFree(_processCtx);
  redisFree(_coorCtx);
}

void Worker::doProcess() {
  redisReply* rReply;
  while (true) {
    cout << "Worker::doProcess" << endl;  
    // will never stop looping
    rReply = (redisReply*)redisCommand(_processCtx, "blpop ag_request 0");
    if (rReply -> type == REDIS_REPLY_NIL) {
      cerr << "Worker::doProcess() get feed back empty queue " << endl;
      //freeReplyObject(rReply);
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      cerr << "Worker::doProcess() get feed back ERROR happens " << endl;
    } else {
      struct timeval time1, time2;
      gettimeofday(&time1, NULL);
      char* reqStr = rReply -> element[1] -> str;
      AGCommand* agCmd = new AGCommand(reqStr);
      int type = agCmd->getType();
      cout << "Worker::doProcess() receive a request of type " << type << endl;
      //agCmd->dump();
      switch (type) {
        case 0: readAndCache(agCmd); break;
        case 1: fetchAndCompute(agCmd); break;
        case 2: concatenate(agCmd); break;
        default:break;
      }
//      gettimeofday(&time2, NULL);
//      cout << "OECWorker::doProcess().duration = " << RedisUtil::duration(time1, time2) << endl;
      // delete agCmd
      delete agCmd;
    }
    // free reply object
    freeReplyObject(rReply); 
  }
}

void Worker::readAndCache(AGCommand* agcmd) {
  cout << "Worker::readDisk!" << endl;

  string blockname = agcmd->getBlockName();
  int blkbytes = agcmd->getBlkBytes();
  int pktbytes = agcmd->getPktBytes();
  int ecw = agcmd->getECW();
  unordered_map<int, int> cid2refs = agcmd->getCid2Refs();
  string stripename = agcmd->getStripeName();

  cout << "Worker::readDisk.blockname: " << blockname << endl;
  cout << "Worker::readDisk.blkbytes: " << blkbytes << endl;
  cout << "Worker::readDisk.pktbytes: " << pktbytes << endl;
  cout << "Worker::readDisk.ecw: " << ecw << endl;
  cout << "Worker::readDisk.cid2refs: ";
  for (auto item: cid2refs) {
      int cid = item.first;
      int ref = item.second;
      cout << "(" << cid << ", " << ref << ") ";
  }
  cout << endl;
  cout << "Worker::readDisk.stripename: " << stripename << endl;

  vector<int> pattern;
  vector<int> indices;
  for (int i=0; i<ecw; i++)
    pattern.push_back(0);
  for (auto item: cid2refs) {
      int cid = item.first;
      indices.push_back(cid);
      int j = cid % ecw;
      pattern[j] = 1;
  }
  sort(indices.begin(), indices.end());
  cout << "Worker::readDisk.pattern: ";
  for (int i=0; i<ecw; i++)
    cout << pattern[i] << " ";
  cout << endl;

  BlockingQueue<DataPacket*>* readqueue = new BlockingQueue<DataPacket*>();

  thread readThread = thread([=]{readWorker(readqueue, blockname, ecw, pattern, blkbytes, pktbytes);});
  thread cacheThread = thread([=]{cacheWorker(readqueue, indices, ecw, stripename, blkbytes, pktbytes);});

  readThread.join();
  cacheThread.join();

  delete readqueue;
}

void Worker::fetchAndCompute(AGCommand* agcmd) {
  cout << "Worker::fetchAndCompute!" << endl;
  vector<int> prevIndices = agcmd->getPrevIndices();
  vector<unsigned int> prevLocs = agcmd->getPrevLocs();
  vector<ComputeTask*> computeTaskList = agcmd->getCTList();
  string stripename = agcmd->getStripeName();
  vector<int> cacheIndices = agcmd->getIndices();
  int ecw = agcmd->getECW();
  int blkbytes = agcmd->getBlkBytes();
  int pktbytes = agcmd->getPktBytes();

  // create blockingqueue for fetching
  BlockingQueue<DataPacket*>** fetchQueue = (BlockingQueue<DataPacket*>**)calloc(prevIndices.size(), sizeof(BlockingQueue<DataPacket*>*));
  for (int i=0; i<prevIndices.size(); i++) 
    fetchQueue[i] = new BlockingQueue<DataPacket*>();
  // create blockingqueue for writing
  BlockingQueue<DataPacket*>** writeQueue = (BlockingQueue<DataPacket*>**)calloc(cacheIndices.size(), sizeof(BlockingQueue<DataPacket*>*));
  for (int i=0; i<cacheIndices.size(); i++)
    writeQueue[i] = new BlockingQueue<DataPacket*>();

  // create fetch thread
  vector<thread> fetchThreads = vector<thread>(prevIndices.size());
  for (int i=0; i<prevIndices.size(); i++) {
    string keybase = stripename+":"+to_string(prevIndices[i]);
    fetchThreads[i] = thread([=]{fetchWorker(fetchQueue[i], keybase, prevLocs[i], ecw, blkbytes, pktbytes);});
  } 

  // create compute thread
  thread computeThread = thread([=]{computeWorker(fetchQueue, prevIndices, writeQueue, cacheIndices, computeTaskList, ecw, blkbytes, pktbytes);});

  // create cache thread
  vector<thread> cacheThreads = vector<thread>(cacheIndices.size());
  for (int i=0; i<cacheIndices.size(); i++) {
    vector<int> tmplist = {cacheIndices[i]};
    cacheThreads[i] = thread([=]{cacheWorker(writeQueue[i], tmplist, ecw, stripename, blkbytes, pktbytes);});
  }

  // join
  for (int i=0; i<prevIndices.size(); i++) {
    fetchThreads[i].join();
  }
  computeThread.join();
  for (int i=0; i<cacheIndices.size(); i++) {
    cacheThreads[i].join();
  }

  // free
  for (int i=0; i<prevIndices.size(); i++)
    delete fetchQueue[i];
  free(fetchQueue);
  for (int i=0; i<cacheIndices.size(); i++)
    delete writeQueue[i];
  free(writeQueue);
  cout << "Worker::fetchAndCompute end!" << endl;
}

void Worker::concatenate(AGCommand* agcmd) {
  cout << "Worker::concatenate!" << endl;
  
  vector<int> prevIndices = agcmd->getPrevIndices();
  vector<unsigned int> prevLocs = agcmd->getPrevLocs();
  string stripename = agcmd->getStripeName();
  string blockname = agcmd->getBlockName();
  int ecw = agcmd->getECW();
  int blkbytes = agcmd->getBlkBytes();
  int pktbytes = agcmd->getPktBytes();

  string fullpath = _conf->_blkDir + "/" + blockname + ".repair";

  // create blockingqueue for fetching
  BlockingQueue<DataPacket*>** fetchQueue = (BlockingQueue<DataPacket*>**)calloc(prevIndices.size(), sizeof(BlockingQueue<DataPacket*>*));
  for (int i=0; i<prevIndices.size(); i++) 
    fetchQueue[i] = new BlockingQueue<DataPacket*>();

  // create fetch thread
  vector<thread> fetchThreads = vector<thread>(prevIndices.size());
  for (int i=0; i<prevIndices.size(); i++) {
    string keybase = stripename+":"+to_string(prevIndices[i]);
    fetchThreads[i] = thread([=]{fetchWorker(fetchQueue[i], keybase, prevLocs[i], ecw, blkbytes, pktbytes);});
  } 

  ofstream ofs(fullpath);
  ofs.close();
  ofs.open(fullpath, ios::app);

  int subpktbytes = pktbytes / ecw;
  int pktnum = blkbytes / pktbytes;

  for (int i=0; i<pktnum; i++) {
    for (int j=0; j<prevIndices.size(); j++) {
      DataPacket* curPkt = fetchQueue[j]->pop();
      int len = curPkt->getDatalen();
      if (len) {
        ofs.write(curPkt->getData(), len);
      }
      else break;
      delete curPkt;
    }
  }

  ofs.close();

  // join
  for (int i=0; i<prevIndices.size(); i++) {
    fetchThreads[i].join();
  }
  cout << "Worker::concatenate fetch ends!" << endl;

  // finish
  redisReply* rReply;
  redisContext* waitCtx = RedisUtil::createContext(_conf->_localIp);
  string wkey = "writefinish:" + blockname;
  int tmpval = htonl(1);
  rReply = (redisReply*)redisCommand(waitCtx, "rpush %s %b", wkey.c_str(), (char*)&tmpval, sizeof(tmpval));
  freeReplyObject(rReply);
  redisFree(waitCtx);

  // free
  for (int i=0; i<prevIndices.size(); i++)
    delete fetchQueue[i];
  free(fetchQueue);
}

void Worker::readWorker(BlockingQueue<DataPacket*>* readqueue, string blockname, int ecw, vector<int> pattern, int blkbytes, int pktbytes) {
  string fullpath = _conf->_blkDir + "/" + blockname;
  cout << "Worker::readWorker:fullpath = " << fullpath << endl;
  
  int fd = open(fullpath.c_str(), O_RDONLY);
  int subpktbytes = pktbytes / ecw;
  int pktnum = blkbytes / pktbytes;
  int readLen = 0, readl = 0;

  for (int i=0; i<pktnum; i++) {
    for (int j=0; j<ecw; j++) {
      if (pattern[j] == 0)
        continue;
      // now we erad the j-th subpacket in packet i
      int start = i * pktbytes + j * subpktbytes;
      readLen = 0;
      DataPacket* curpkt = new DataPacket(subpktbytes);
      while (readLen < subpktbytes) {
        if ((readl = pread(fd, 
                           curpkt->getData() + readLen, 
                           subpktbytes - readLen, 
                           start)) < 0) {
          cout << "ERROR during disk read" << endl;
        } else {
          readLen += readl;
        }
      }
      readqueue->push(curpkt);
    } 
  }
  close(fd);
}

void Worker::cacheWorker(BlockingQueue<DataPacket*>* cachequeue, vector<int> idxlist, int ecw, string keybase, int blkbytes, int pktbytes) {
  struct timeval time1, time2;
  gettimeofday(&time1, NULL);

  redisContext* writeCtx = RedisUtil::createContext(_conf->_localIp);
  redisReply* rReply;

  int subpktbytes = pktbytes / ecw;
  int pktnum = blkbytes / pktbytes;

  int count=0;
  int replyid=0;
  for (int i=0; i<pktnum; i++) {
    for (int j=0; j<idxlist.size(); j++) {
      DataPacket* curslice = cachequeue->pop();
      string key = keybase+":"+to_string(idxlist[j])+":"+to_string(i);  
      
      // TODO: add references
      int refnum = 1;

      int len = curslice->getDatalen();
      char* raw = curslice->getRaw();
      int rawlen = len + 4;
      for (int k=0; k<refnum; k++) {
        redisAppendCommand(writeCtx, "RPUSH %s %b", key.c_str(), raw, rawlen); count++;
      }
      delete curslice;
      if (i>1) {
        redisGetReply(writeCtx, (void**)&rReply); replyid++;
        freeReplyObject(rReply);
      }
    }
  }
  for (int i=replyid; i<count; i++)  {
    redisGetReply(writeCtx, (void**)&rReply); replyid++;
    freeReplyObject(rReply);
  }
  
  gettimeofday(&time2, NULL);
  cout << "Worker::cacheWorker.duration: " << DistUtil::duration(time1, time2) << " for " << keybase << ":";
  for (int i=0; i<idxlist.size(); i++) {
      cout << idxlist[i] << " ";
  }
  cout << endl;
}

void Worker::fetchWorker(BlockingQueue<DataPacket*>* fetchQueue,
                     string keybase,
                     unsigned int loc,
                     int ecw,
                     int blkbytes, 
                     int pktbytes) {
  int pktnum = blkbytes / pktbytes;
  int slicesize = pktbytes / ecw;

  struct timeval time1, time2;
  gettimeofday(&time1, NULL);

  if (loc == 0) {
      cout << "Worker::fetchWorker generates zero bytes for " << keybase << endl;
      for (int i=0; i<pktnum; i++) {
          DataPacket* pkt = new DataPacket(slicesize);
          fetchQueue->push(pkt);
      }
  } else {
    redisReply* rReply;
    redisContext* fetchCtx = RedisUtil::createContext(loc);

    int replyid=0;
    for (int i=0; i<pktnum; i++) {
      string key = keybase+":"+to_string(i);
      redisAppendCommand(fetchCtx, "blpop %s 0", key.c_str());
    }
    
    struct timeval t1, t2;
    double t;
    for (int i=replyid; i<pktnum; i++) {
      string key = keybase+":"+to_string(i);
      gettimeofday(&t1, NULL);
      redisGetReply(fetchCtx, (void**)&rReply);
      gettimeofday(&t2, NULL);
      //if (i == 0) cout << "OECWorker::fetchWorker.fetch first t = " << RedisUtil::duration(t1, t2) << endl;
      char* content = rReply->element[1]->str;
      DataPacket* pkt = new DataPacket(content);
      int curDataLen = pkt->getDatalen();
      fetchQueue->push(pkt);
      freeReplyObject(rReply);
    }
    gettimeofday(&time2, NULL);
    cout << "Worker::fetchWorker.duration: " << DistUtil::duration(time1, time2) << " for " << keybase << endl;
    redisFree(fetchCtx);
  }
}

void Worker::computeWorker(BlockingQueue<DataPacket*>** fetchQueue,
                           vector<int> fetchIndices,
                           BlockingQueue<DataPacket*>** writeQueue,
                           vector<int> writeIndices,
                           vector<ComputeTask*> ctlist, 
                           int ecw, int blkbytes, int pktbytes) {
  struct timeval time1, time2, time3;
  gettimeofday(&time1, NULL);

  int subpktbytes = pktbytes / ecw;
  int pktnum = blkbytes / pktbytes;
   
  for (int i=0; i<pktnum; i++) {
    unordered_map<int, char*> bufMap;
    unordered_map<int, DataPacket*> pktMap;
    
    // read from fetchQueue
    for (int j=0; j<fetchIndices.size(); j++) {
      int curidx = fetchIndices[j];
      DataPacket* curpkt = fetchQueue[j]->pop();
      pktMap.insert(make_pair(curidx, curpkt));
      bufMap.insert(make_pair(curidx, curpkt->getData())); 
    }
    // prepare data pkt for writeQueue
    for (int j=0; j<writeIndices.size(); j++) {
      int curidx = writeIndices[j];
      DataPacket* curpkt = new DataPacket(subpktbytes);
      pktMap.insert(make_pair(curidx, curpkt));
      bufMap.insert(make_pair(curidx, curpkt->getData()));
    }

    // iterate through ctlist
    for (int ctidx=0; ctidx<ctlist.size(); ctidx++) {
      ComputeTask* curtask = ctlist[ctidx];
      vector<int> srclist = curtask->_srclist;
      vector<int> dstlist = curtask->_dstlist;
      vector<vector<int>> coefs = curtask->_coefs;

      // make sure that index in srclits has been in bufmap
      for(auto srcidx: srclist)
        assert(bufMap.find(srcidx) != bufMap.end());
      // now we create buf in bufMap
      for (auto dstidx: dstlist) {
        if (bufMap.find(dstidx) == bufMap.end()) {
          char* tmpbuf = (char*)calloc(subpktbytes, sizeof(char));
          bufMap.insert(make_pair(dstidx, tmpbuf));
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
        assert (bufMap.find(child) != bufMap.end());
        data[bufIdx] = bufMap[child];
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
      Computation::Multi(code, data, matrix, row, col, subpktbytes, "Isal"); 
    }
    // now the computation is finished
    // we write pkts into writeQueue
    unordered_map<int, int> ignore;
    for (int wi=0; wi<writeIndices.size(); wi++) {
      int curidx = writeIndices[wi];
      DataPacket* curpkt = pktMap[curidx];
      writeQueue[wi]->push(curpkt);
      ignore.insert(make_pair(curidx, 1));
    }
    // free data packet
    for (auto di=0; di<fetchIndices.size(); di++) {
      int curidx = fetchIndices[di];
      DataPacket* curpkt = pktMap[curidx];
      delete curpkt;
      ignore.insert(make_pair(curidx, 1));
    }
    // free bufMap
    for (auto item: bufMap) {
      int curidx = item.first;
      if(ignore.find(curidx) == ignore.end() && item.second)
        free(item.second);
    }
    bufMap.clear();
    pktMap.clear();
  }
}
