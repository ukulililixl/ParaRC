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
        case 3: fetchAndCompute2(agCmd); break;
        case 4: concatenate2(agCmd); break;
        case 5: readAndCompute(agCmd); break;
        case 6: readAndCacheWithOffset(agCmd); break;
        case 7: readAndComputeWithOffset(agCmd); break;
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
  //cout << "Worker::readDisk!" << endl;
  struct timeval time1, time2, time3;  
  gettimeofday(&time1, NULL); 

  string blockname = agcmd->getBlockName();
  int blkbytes = agcmd->getBlkBytes();
  int pktbytes = agcmd->getPktBytes();
  int ecw = agcmd->getECW();
  unordered_map<int, int> cid2refs = agcmd->getCid2Refs();
  string stripename = agcmd->getStripeName();

  //cout << "Worker::readDisk.blockname: " << blockname << endl;
  //cout << "Worker::readDisk.blkbytes: " << blkbytes << endl;
  //cout << "Worker::readDisk.pktbytes: " << pktbytes << endl;
  //cout << "Worker::readDisk.ecw: " << ecw << endl;
  //cout << "Worker::readDisk.cid2refs: ";
  //for (auto item: cid2refs) {
  //    int cid = item.first;
  //    int ref = item.second;
  //    cout << "(" << cid << ", " << ref << ") ";
  //}
  //cout << endl;
  //cout << "Worker::readDisk.stripename: " << stripename << endl;

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
  thread cacheThread = thread([=]{cacheWorker(readqueue, indices, ecw, stripename, blkbytes, pktbytes, cid2refs);});

  readThread.join();
  cacheThread.join();

  delete readqueue;

  gettimeofday(&time2, NULL); 
  //cout << "Worker::readDisk.duration: " << DistUtil::duration(time1, time2) << endl;
}

void Worker::readAndCacheWithOffset(AGCommand* agcmd) {
  //cout << "Worker::readDisk!" << endl;
  struct timeval time1, time2, time3;  
  gettimeofday(&time1, NULL); 

  string blockname = agcmd->getBlockName();
  int blkbytes = agcmd->getBlkBytes();
  int pktbytes = agcmd->getPktBytes();
  int ecw = agcmd->getECW();
  unordered_map<int, int> cid2refs = agcmd->getCid2Refs();
  string stripename = agcmd->getStripeName();
  int offset = agcmd->getOffset();

  //cout << "Worker::readAndCacheWithOffset.blockname: " << blockname << endl;
  //cout << "Worker::readAndCacheWithOffset.blkbytes: " << blkbytes << endl;
  //cout << "Worker::readAndCacheWithOffset.pktbytes: " << pktbytes << endl;
  //cout << "Worker::readAndCacheWithOffset.ecw: " << ecw << endl;
  //cout << "Worker::readAndCacheWithOffset.cid2refs: ";
  //for (auto item: cid2refs) {
  //    int cid = item.first;
  //    int ref = item.second;
  //    cout << "(" << cid << ", " << ref << ") ";
  //}
  //cout << endl;
  //cout << "Worker::readAndCacheWithOffset.stripename: " << stripename << endl;
  //cout << "Worker::readAndCacheWithOffset.offset: " << offset << endl;

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

  thread readThread = thread([=]{readWorkerWithOffset(readqueue, blockname, ecw, pattern, blkbytes, pktbytes, offset);});
  thread cacheThread = thread([=]{cacheWorker(readqueue, indices, ecw, stripename, blkbytes, pktbytes, cid2refs);});

  readThread.join();
  cacheThread.join();

  delete readqueue;

  gettimeofday(&time2, NULL); 
  //cout << "Worker::readDisk.duration: " << DistUtil::duration(time1, time2) << endl;
}

void Worker::fetchAndCompute(AGCommand* agcmd) {
  // cout << "Worker::fetchAndCompute!" << endl;
  struct timeval time1, time2, time3;                                                                                                                                                                    
  gettimeofday(&time1, NULL); 
  vector<int> prevIndices = agcmd->getPrevIndices();
  vector<unsigned int> prevLocs = agcmd->getPrevLocs();
  vector<ComputeTask*> computeTaskList = agcmd->getCTList();
  string stripename = agcmd->getStripeName();
  unordered_map<int, int> cid2refs = agcmd->getCid2Refs();
  int ecw = agcmd->getECW();
  int blkbytes = agcmd->getBlkBytes();
  int pktbytes = agcmd->getPktBytes();

  vector<int> cacheIndices;
  for (auto item: cid2refs) {
      int cid = item.first;
      cacheIndices.push_back(cid);
  }
  
  //if (find(prevIndices.begin(), prevIndices.end(), 65) != prevIndices.end()) {
  //  debugFetchAndCompute(agcmd);
  //} else {

    // // debug
    // cout << "Worker::fetchAndCompute prev: ";
    // for (int i=0; i<prevIndices.size(); i++)
    //     cout << "(" << prevIndices[i] << ", " << RedisUtil::ip2Str(prevLocs[i]) << ") ";

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
    gettimeofday(&time2, NULL); 
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
      cacheThreads[i] = thread([=]{cacheWorker(writeQueue[i], tmplist, ecw, stripename, blkbytes, pktbytes, cid2refs);});
    }

    // join
    for (int i=0; i<prevIndices.size(); i++) {
      fetchThreads[i].join();
    }
    gettimeofday(&time3, NULL); 
    cout << "Worker::fetchAndCompute.overall fetch time: " << DistUtil::duration(time2, time3) << endl;
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
  
  //}
}

void Worker::fetchAndCompute2(AGCommand* agcmd) {
    cout << "Worker::fetchAndCompute2" << endl;
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);

    int nFetchStream = agcmd->getNFetchStream();
    int nCompute = agcmd->getNCompute();
    string stripename = agcmd->getStripeName();
    int nOutCids = agcmd->getNOutCids();
    int ecw = agcmd->getECW();
    int blkbytes = agcmd->getBlkBytes();
    int pktbytes = agcmd->getPktBytes();
    int taskid = agcmd->getTaskid();

    if (ecw > 1)
        fetchAndCompute3(agcmd);
    else {
    
    cout << "Worker::fetchAndCompute2:nFetchStream = " << nFetchStream << endl;
    cout << "Worker::fetchAndCompute2:nCompute = " << nCompute << endl;
    cout << "Worker::fetchAndCompute2:stripename = " << stripename << endl;
    cout << "Worker::fetchAndCompute2:nOutCids = " << nOutCids << endl;
    cout << "Worker::fetchAndCompute2:ecw = " << ecw << endl;
    cout << "Worker::fetchAndCompute2:blkbytes = " << blkbytes << endl;
    cout << "Worker::fetchAndCompute2:pktbytes = " << pktbytes << endl;
    cout << "Worker::fetchAndCompute2:taskid = " << taskid << endl;

    redisContext* redisCtx = RedisUtil::createContext(_conf -> _localIp);
    redisReply* rReply;

    // read FetchCommands
    unordered_map<unsigned int, vector<int>> ip2cidlist;
    unordered_map<int, BlockingQueue<DataPacket*>*> fetchmap;
    string rkey = stripename + ":task"+to_string(taskid)+":fetch";
    for (int i=0; i<nFetchStream; i++) {
        rReply = (redisReply*)redisCommand(redisCtx, "blpop %s 0", rkey.c_str());
        char* reqStr = rReply -> element[1] -> str;
        FetchCommand* fetchCmd = new FetchCommand(reqStr);
        unsigned int ip = fetchCmd->getFetchIp();
        vector<int> cidlist = fetchCmd->getCidList();
        ip2cidlist.insert(make_pair(ip, cidlist));

        for (int j=0; j<cidlist.size(); j++) {
            int cid = cidlist[j];
            BlockingQueue<DataPacket*>* readqueue = new BlockingQueue<DataPacket*>();
            fetchmap.insert(make_pair(cid, readqueue));
        }

        freeReplyObject(rReply);
        fetchCmd->setCmd(NULL, 0);
        delete fetchCmd;
    }

    // create fetchthreads
    gettimeofday(&time2, NULL);
    vector<thread> fetchThreads = vector<thread>(ip2cidlist.size());
    int fetchThreadsIdx = 0;
    for (auto item: ip2cidlist) {
        unsigned int ip = item.first;
        vector<int> cidlist = item.second;
        unordered_map<int, BlockingQueue<DataPacket*>*> curfetchmap;
        for (auto cid: cidlist) {
            curfetchmap.insert(make_pair(cid, fetchmap[cid]));
        }
        fetchThreads[fetchThreadsIdx++] = thread([=]{fetchWorker2(curfetchmap, stripename, ip, ecw, blkbytes, pktbytes);});
    }

    // read compute commands
    vector<ComputeTask*> computeTaskList;
    rkey = stripename + ":task" + to_string(taskid) + ":compute";
    for (int i=0; i<nCompute; i++) {
        rReply = (redisReply*)redisCommand(redisCtx, "blpop %s 0", rkey.c_str());
        char* reqStr = rReply -> element[1] -> str;
        ComputeCommand* computeCmd = new ComputeCommand(reqStr);
        vector<int> srclist = computeCmd->getSrcList();
        vector<int> dstlist = computeCmd->getDstList();
        vector<vector<int>> coefs = computeCmd->getCoefs();

        ComputeTask* ct = new ComputeTask(srclist, dstlist, coefs);
        computeTaskList.push_back(ct);

        // cout << "Compute: ( ";
        // for (auto tmpid: srclist)
        //     cout << tmpid << " ";
        // cout << ")->( ";
        // for (auto tmpid: dstlist)
        //     cout << tmpid << " ";
        // cout << ")" << endl;

        freeReplyObject(rReply);
        computeCmd->setCmd(NULL, 0);
        delete computeCmd;
    }

    // read cache command
    rkey = stripename + ":task" + to_string(taskid) + ":cache";
    rReply = (redisReply*)redisCommand(redisCtx, "blpop %s 0", rkey.c_str());
    char* reqStr = rReply -> element[1] -> str;
    CacheCommand* cacheCmd = new CacheCommand(reqStr);
    unordered_map<int, int> cacheRefs = cacheCmd->getCid2Refs();
    cacheCmd->setCmd(NULL, 0);
    freeReplyObject(rReply);
    delete cacheCmd;

    // cout << "cache: ";
    // for (auto tmpitem: cacheRefs)
    //     cout << "(" << tmpitem.first << "," << tmpitem.second << ") ";
    // cout << endl;

    unordered_map<int, BlockingQueue<DataPacket*>*> cachemap;
    for (auto item: cacheRefs) {
        int cid = item.first;
        BlockingQueue<DataPacket*>* queue = new BlockingQueue<DataPacket*>();
        cachemap.insert(make_pair(cid, queue));
    }

    // create compute thread
    thread computeThread = thread([=]{computeWorker2(fetchmap, cachemap, computeTaskList, ecw, blkbytes, pktbytes);});

    // create cache thread
    thread cacheThread = thread([=]{cacheWorker2(cachemap, cacheRefs, ecw, stripename, blkbytes, pktbytes);});

    // join
    for (int i=0; i<fetchThreads.size(); i++) {
        fetchThreads[i].join();
    }
    gettimeofday(&time3, NULL);
    cout << "Worker::fetchAndCompute2.fetch data duration: " << DistUtil::duration(time2, time3) << endl;
    computeThread.join();
    gettimeofday(&time3, NULL);
    cout << "Worker::fetchAndCompute2.compute duration: " << DistUtil::duration(time2, time3) << endl;
    cacheThread.join();

    // free
    free(redisCtx);
    for (auto item: fetchmap) {
        delete item.second;
    }
    for (auto item: cachemap) {
        delete item.second;
    }
    
  }
}

void Worker::fetchAndCompute3(AGCommand* agcmd) {
    cout << "Worker::fetchAndCompute3" << endl;
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);

    int nFetchStream = agcmd->getNFetchStream();
    int nCompute = agcmd->getNCompute();
    string stripename = agcmd->getStripeName();
    int nOutCids = agcmd->getNOutCids();
    int ecw = agcmd->getECW();
    int blkbytes = agcmd->getBlkBytes();
    int pktbytes = agcmd->getPktBytes();
    int taskid = agcmd->getTaskid();
    
    cout << "Worker::fetchAndCompute3:nFetchStream = " << nFetchStream << endl;
    cout << "Worker::fetchAndCompute3:nCompute = " << nCompute << endl;
    cout << "Worker::fetchAndCompute3:stripename = " << stripename << endl;
    cout << "Worker::fetchAndCompute3:nOutCids = " << nOutCids << endl;
    cout << "Worker::fetchAndCompute3:ecw = " << ecw << endl;
    cout << "Worker::fetchAndCompute3:blkbytes = " << blkbytes << endl;
    cout << "Worker::fetchAndCompute3:pktbytes = " << pktbytes << endl;
    cout << "Worker::fetchAndCompute3:taskid = " << taskid << endl;

    redisContext* redisCtx = RedisUtil::createContext(_conf -> _localIp);
    redisReply* rReply;

    // fetch 
    unordered_map<unsigned int, vector<int>> ip2cidlist;
    unordered_map<unsigned int, BlockingQueue<DataPacket*>*> fetchmap;
    string rkey = stripename + ":task"+to_string(taskid)+":fetch";
    for (int i=0; i<nFetchStream; i++) {
        rReply = (redisReply*)redisCommand(redisCtx, "blpop %s 0", rkey.c_str());
        char* reqStr = rReply -> element[1] -> str;
        FetchCommand* fetchCmd = new FetchCommand(reqStr);
        unsigned int ip = fetchCmd->getFetchIp();
        vector<int> cidlist = fetchCmd->getCidList();
        ip2cidlist.insert(make_pair(ip, cidlist));

        BlockingQueue<DataPacket*>* readqueue = new BlockingQueue<DataPacket*>();
        fetchmap.insert(make_pair(ip, readqueue));

        freeReplyObject(rReply);
        fetchCmd->setCmd(NULL, 0);
        delete fetchCmd;
    }

    // create fetchthreads
    gettimeofday(&time2, NULL);
    vector<thread> fetchThreads = vector<thread>(ip2cidlist.size());
    int fetchThreadsIdx = 0;
    for (auto item: ip2cidlist) {
        unsigned int ip = item.first;
        vector<int> cidlist = item.second;
        BlockingQueue<DataPacket*>* queue = fetchmap[ip];
        fetchThreads[fetchThreadsIdx++] = thread([=]{fetchWorker3(queue, stripename, cidlist, ip, ecw, blkbytes, pktbytes);});
    }

    // read compute commands
    vector<ComputeTask*> computeTaskList;
    rkey = stripename + ":task" + to_string(taskid) + ":compute";
    for (int i=0; i<nCompute; i++) {
        rReply = (redisReply*)redisCommand(redisCtx, "blpop %s 0", rkey.c_str());
        char* reqStr = rReply -> element[1] -> str;
        ComputeCommand* computeCmd = new ComputeCommand(reqStr);
        vector<int> srclist = computeCmd->getSrcList();
        vector<int> dstlist = computeCmd->getDstList();
        vector<vector<int>> coefs = computeCmd->getCoefs();

        ComputeTask* ct = new ComputeTask(srclist, dstlist, coefs);
        computeTaskList.push_back(ct);

        // cout << "Compute: ( ";
        // for (auto tmpid: srclist)
        //     cout << tmpid << " ";
        // cout << ")->( ";
        // for (auto tmpid: dstlist)
        //     cout << tmpid << " ";
        // cout << ")" << endl;

        freeReplyObject(rReply);
        computeCmd->setCmd(NULL, 0);
        delete computeCmd;
    }

    // read cache command
    rkey = stripename + ":task" + to_string(taskid) + ":cache";
    rReply = (redisReply*)redisCommand(redisCtx, "blpop %s 0", rkey.c_str());
    char* reqStr = rReply -> element[1] -> str;
    CacheCommand* cacheCmd = new CacheCommand(reqStr);
    unordered_map<int, int> cacheRefs = cacheCmd->getCid2Refs();
    cacheCmd->setCmd(NULL, 0);
    freeReplyObject(rReply);
    delete cacheCmd;

    // cout << "cache: ";
    // for (auto tmpitem: cacheRefs)
    //     cout << "(" << tmpitem.first << "," << tmpitem.second << ") ";
    // cout << endl;

    BlockingQueue<DataPacket*>* cachequeue = new BlockingQueue<DataPacket*>();
    vector<int> cachecidlist;
    for (auto item: cacheRefs) {
        int cid = item.first;
        cachecidlist.push_back(cid);
    }

    // create compute thread
    thread computeThread = thread([=]{computeWorker3(fetchmap, ip2cidlist, cachequeue, cachecidlist, computeTaskList, ecw, blkbytes, pktbytes);});

    // create cache thread
    thread cacheThread = thread([=]{cacheWorker3(cachequeue, cachecidlist, cacheRefs, ecw, stripename, blkbytes, pktbytes);});

    // join
    for (int i=0; i<fetchThreads.size(); i++) {
        fetchThreads[i].join();
    }
    gettimeofday(&time3, NULL);
    cout << "Worker::fetchAndCompute2.fetch data duration: " << DistUtil::duration(time2, time3) << endl;
    computeThread.join();
    gettimeofday(&time3, NULL);
    cout << "Worker::fetchAndCompute2.compute duration: " << DistUtil::duration(time2, time3) << endl;
    cacheThread.join();

//    // free
//    free(redisCtx);
//    for (auto item: fetchmap) {
//        delete item.second;
//    }
//    delete cachequeue;
}

void Worker::debugFetchAndCompute(AGCommand* agcmd) {
  cout << "Worker::debugFetchAndCompute!" << endl;

  vector<int> prevIndices = agcmd->getPrevIndices();
  cout << "Worker::debugFetchAndCompute! prevIndices: ";
  for (int i=0; i<prevIndices.size(); i++)
      cout << prevIndices[i] << " ";
  cout << endl;
  
  vector<unsigned int> prevLocs = agcmd->getPrevLocs();
  cout << "Worker::debugFetchAndCompute! prevLocs: ";
  for (int i=0; i<prevLocs.size(); i++)
      cout << RedisUtil::ip2Str(prevLocs[i]) << " ";
  cout << endl;

  vector<ComputeTask*> computeTaskList = agcmd->getCTList();

  string stripename = agcmd->getStripeName();
  cout << "Worker::debugFetchAndCompute! stripename: " << stripename << endl;

  vector<int> cacheIndices = agcmd->getIndices();
  cout << "Worker::debugFetchAndCompute! cacheIndices: ";
  for (int i=0; i<cacheIndices.size(); i++) {
      cout << cacheIndices[i] << " ";
  }
  cout << endl;

  int ecw = agcmd->getECW();
  int blkbytes = agcmd->getBlkBytes();
  int pktbytes = agcmd->getPktBytes();
  cout << "Worker::debugFetchAndCompute! ecw: " << ecw << ", blkbytes: " << blkbytes << ", pktbytes: " << pktbytes << endl;

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
  unordered_map<int, int> cid2refs;
  for (int i=0; i<cacheIndices.size(); i++) {
    vector<int> tmplist = {cacheIndices[i]};
    cid2refs.insert(make_pair(cacheIndices[i], 1));
    cacheThreads[i] = thread([=]{cacheWorker(writeQueue[i], tmplist, ecw, stripename, blkbytes, pktbytes, cid2refs);});
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
  // string fullpath = DistUtil::getFullPathForBlock(_conf->_blkDir, blockname) + ".repair";

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

void Worker::concatenate2(AGCommand* agcmd) {

    int nFetchStream = agcmd->getNFetchStream();
    string stripename = agcmd->getStripeName();
    string blockname = agcmd->getBlockName();
    int ecw = agcmd->getECW();
    int blkbytes = agcmd->getBlkBytes();
    int pktbytes = agcmd->getPktBytes();
    int taskid = agcmd->getTaskid();

    string fullpath = _conf->_blkDir + "/" + blockname + ".repair";
    // string fullpath = DistUtil::getFullPathForBlock(_conf->_blkDir, blockname) + ".repair";

    //cout << "Worker::concatenate2:nFetchStream = " << nFetchStream << endl;
    //cout << "Worker::concatenate2:stripename = " << stripename << endl;
    //cout << "Worker::concatenate2:blockname = " << blockname << endl;
    //cout << "Worker::concatenate2:ecw = " << ecw << endl;
    //cout << "Worker::concatenate2:blkbytes = " << blkbytes << endl;
    //cout << "Worker::concatenate2:pktbytes = " << pktbytes << endl;
    //cout << "Worker::concatenate2:taskid = " << taskid << endl;

    redisContext* redisCtx = RedisUtil::createContext(_conf -> _localIp);
    redisReply* rReply;

    // read FetchCommands
    unordered_map<unsigned int, vector<int>> ip2cidlist;
    unordered_map<int, BlockingQueue<DataPacket*>*> fetchmap;
    string rkey = stripename + ":task"+to_string(taskid)+":fetch";
    for (int i=0; i<nFetchStream; i++) {
        rReply = (redisReply*)redisCommand(redisCtx, "blpop %s 0", rkey.c_str());
        char* reqStr = rReply -> element[1] -> str;
        FetchCommand* fetchCmd = new FetchCommand(reqStr);
        unsigned int ip = fetchCmd->getFetchIp();
        vector<int> cidlist = fetchCmd->getCidList();
        ip2cidlist.insert(make_pair(ip, cidlist));

        for (int j=0; j<cidlist.size(); j++) {
            int cid = cidlist[j];
            BlockingQueue<DataPacket*>* readqueue = new BlockingQueue<DataPacket*>();
            fetchmap.insert(make_pair(cid, readqueue));
        }

        freeReplyObject(rReply);
        fetchCmd->setCmd(NULL, 0);
        delete fetchCmd;
    }

    // create fetchthreads
    vector<thread> fetchThreads = vector<thread>(ip2cidlist.size());
    int fetchThreadsIdx = 0;
    for (auto item: ip2cidlist) {
        unsigned int ip = item.first;
        vector<int> cidlist = item.second;
        unordered_map<int, BlockingQueue<DataPacket*>*> curfetchmap;
        for (auto cid: cidlist) {
            curfetchmap.insert(make_pair(cid, fetchmap[cid]));
        }
        fetchThreads[fetchThreadsIdx++] = thread([=]{fetchWorker2(curfetchmap, stripename, ip, ecw, blkbytes, pktbytes);});
    }

    ofstream ofs(fullpath);
    ofs.close();
    ofs.open(fullpath, ios::app);

    int subpktbytes = pktbytes / ecw;
    int pktnum = blkbytes / pktbytes;

    vector<int> prevIndices;
    for(auto item: fetchmap) {
        int cid = item.first;
        prevIndices.push_back(cid);
    }
    sort(prevIndices.begin(), prevIndices.end());

    for (int i=0; i<pktnum; i++) {
        for (int j=0; j<prevIndices.size(); j++) {
            int cid = prevIndices[j];
            DataPacket* curPkt = fetchmap[cid]->pop();
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
    for (int i=0; i<fetchThreads.size(); i++) {
        fetchThreads[i].join();
    }
    cout << "Worker::concatenate fetch ends!" << endl;
    free(redisCtx);

    // finish
    redisReply* wReply;
    redisContext* waitCtx = RedisUtil::createContext(_conf->_localIp);
    string wkey = "writefinish:" + blockname;
    int tmpval = htonl(1);
    wReply = (redisReply*)redisCommand(waitCtx, "rpush %s %b", wkey.c_str(), (char*)&tmpval, sizeof(tmpval));
    freeReplyObject(wReply);
    redisFree(waitCtx);

    // free
    for (auto item: fetchmap) {
        delete item.second;
    }
}

void Worker::readWorker(BlockingQueue<DataPacket*>* readqueue, string blockname, int ecw, vector<int> pattern, int blkbytes, int pktbytes) {
  // string fullpath = _conf->_blkDir + "/" + blockname;
  string fullpath = DistUtil::getFullPathForBlock(_conf->_blkDir, blockname);
  cout << "Worker::readWorker:fullpath = " << fullpath << endl;
  
  int fd = open(fullpath.c_str(), O_RDONLY);
  int subpktbytes = pktbytes / ecw;
  int pktnum = blkbytes / pktbytes;
  int readLen = 0, readl = 0;

  struct timeval time1, time2, time3;                                                                                                                                                                    
  gettimeofday(&time1, NULL); 
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
  gettimeofday(&time2, NULL); 
  cout << "Worker::readWorker.duration: " << DistUtil::duration(time1, time2) << endl;
}

void Worker::readWorkerWithOffset(BlockingQueue<DataPacket*>* readqueue, string blockname, int ecw, vector<int> pattern, int blkbytes, int pktbytes, int offset) {
  // string fullpath = _conf->_blkDir + "/" + blockname;
  string fullpath = DistUtil::getFullPathForBlock(_conf->_blkDir, blockname);
  cout << "Worker::readWorker:fullpath = " << fullpath << endl;
  
  int fd = open(fullpath.c_str(), O_RDONLY);
  int subpktbytes = pktbytes / ecw;
  int pktnum = blkbytes / pktbytes;
  int readLen = 0, readl = 0;
  
  struct timeval time1, time2, time3;
  gettimeofday(&time1, NULL); 
  for (int i=0; i<pktnum; i++) {
    for (int j=0; j<ecw; j++) {
      if (pattern[j] == 0)
        continue;
      // now we erad the j-th subpacket in packet i
      int start = offset + i * pktbytes + j * subpktbytes;
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
  gettimeofday(&time2, NULL); 
  cout << "Worker::readWorker.duration: " << DistUtil::duration(time1, time2) << endl;
}

void Worker::readWorker(unordered_map<int, BlockingQueue<DataPacket*>*> readmap,
        string blockname, int ecw, vector<int> pattern, vector<int> patternidx,
        int blkbytes, int pktbytes) {
    // string fullpath = _conf->_blkDir + "/" + blockname;
    string fullpath = DistUtil::getFullPathForBlock(_conf->_blkDir, blockname);
    cout << "Worker::readWorker:fullpath = " << fullpath << endl;
    
    int fd = open(fullpath.c_str(), O_RDONLY);
    int subpktbytes = pktbytes / ecw;
    int pktnum = blkbytes / pktbytes;
    int readLen = 0, readl = 0;
    
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL); 
    for (int i=0; i<pktnum; i++) {
        for (int j=0; j<ecw; j++) {
            if (pattern[j] == 0)
                continue;
            int cid = patternidx[j];
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
            readmap[cid]->push(curpkt);
        } 
    }
    close(fd);
    gettimeofday(&time2, NULL); 
    cout << "Worker::readWorker.duration: " << DistUtil::duration(time1, time2) << endl;
}

void Worker::readWorkerWithOffset(unordered_map<int, BlockingQueue<DataPacket*>*> readmap,
        string blockname, int ecw, vector<int> pattern, vector<int> patternidx,
        int blkbytes, int pktbytes, int offset) {
    // string fullpath = _conf->_blkDir + "/" + blockname;
    string fullpath = DistUtil::getFullPathForBlock(_conf->_blkDir, blockname);
    cout << "Worker::readWorker:fullpath = " << fullpath << endl;
    
    int fd = open(fullpath.c_str(), O_RDONLY);
    int subpktbytes = pktbytes / ecw;
    int pktnum = blkbytes / pktbytes;
    int readLen = 0, readl = 0;
    
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL); 
    for (int i=0; i<pktnum; i++) {
        for (int j=0; j<ecw; j++) {
            if (pattern[j] == 0)
                continue;
            int cid = patternidx[j];
            // now we erad the j-th subpacket in packet i
            int start = offset + i * pktbytes + j * subpktbytes;
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
            readmap[cid]->push(curpkt);
        } 
    }
    close(fd);
    gettimeofday(&time2, NULL); 
    cout << "Worker::readWorker.duration: " << DistUtil::duration(time1, time2) << endl;
}

void Worker::cacheWorker(BlockingQueue<DataPacket*>* cachequeue, vector<int> idxlist, int ecw, string keybase, int blkbytes, int pktbytes, unordered_map<int, int> cid2refs) {
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
      int cid = idxlist[j];
      int ref = cid2refs[cid];
      //int ref = 1;
      //if (cid2refs.find(cid) != cid2refs.end())
      //    ref = cid2refs[cid];

      //string key = keybase+":"+to_string(idxlist[j])+":"+to_string(i);  
      string key = keybase+":"+to_string(idxlist[j]);
      
      int len = curslice->getDatalen();
      char* raw = curslice->getRaw();
      int rawlen = len + 4;
      for (int k=0; k<ref; k++) {
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
  //for (int i=0; i<idxlist.size(); i++) {
  //    cout << idxlist[i] << " ";
  //}
  cout << endl;
  redisFree(writeCtx);
}

void Worker::cacheWorker2(
        unordered_map<int, BlockingQueue<DataPacket*>*> cacheMap,
        unordered_map<int, int> cacheRefs,
        int ecw, string stripename,
        int blkbytes, int pktbytes) {
  struct timeval time1, time2;
  gettimeofday(&time1, NULL);

  redisContext* writeCtx = RedisUtil::createContext(_conf->_localIp);
  redisReply* rReply;

  int subpktbytes = pktbytes / ecw;
  int pktnum = blkbytes / pktbytes;

  int count=0;
  int replyid=0;
  for (int i=0; i<pktnum; i++) {
      for (auto item: cacheMap) {
          int cid = item.first;
          BlockingQueue<DataPacket*>* queue = cacheMap[cid];
          int ref = cacheRefs[cid];

          string key = stripename + ":" + to_string(cid);

          DataPacket* curslice = queue->pop();
          int len = curslice->getDatalen();
          char* raw = curslice->getRaw();
          int rawlen = len + 4;
          for (int k=0; k<ref; k++) {
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
  cout << "Worker::cacheWorker.duration: " << DistUtil::duration(time1, time2) << " for " << stripename << ":";
  for (auto item: cacheRefs) {
      cout << item.first << " ";
  }
  cout << endl;
  redisFree(writeCtx);
}

void Worker::cacheWorker3(BlockingQueue<DataPacket*>* cachequeue,
        vector<int> cachelist,
        unordered_map<int, int> cacheRefs,
        int ecw, string stripename,
        int blkbytes, int pktbytes) {
  struct timeval time1, time2;
  gettimeofday(&time1, NULL);

  redisContext* writeCtx = RedisUtil::createContext(_conf->_localIp);
  redisReply* rReply;

  int subpktbytes = pktbytes / ecw;
  int pktnum = blkbytes / pktbytes;

  int count=0;
  int replyid=0;
  for (int i=0; i<pktnum; i++) {
      for (int tmpi=0; tmpi<cachelist.size(); tmpi++) {
          int cid = cachelist[tmpi];
          int ref = cacheRefs[cid];
          string key = stripename + ":" + to_string(cid);
          DataPacket* curslice = cachequeue->pop();
          int len = curslice->getDatalen();
          char* raw = curslice->getRaw();
          int rawlen = len + 4;
          for (int k=0; k<ref; k++) {
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
  cout << "Worker::cacheWorker.duration: " << DistUtil::duration(time1, time2) << " for " << stripename << ":";
  for (auto item: cacheRefs) {
      cout << item.first << " ";
  }
  cout << endl;
  redisFree(writeCtx);
}

void Worker::fetchWorker(BlockingQueue<DataPacket*>* fetchQueue,
                     string keybase,
                     unsigned int loc,
                     int ecw,
                     int blkbytes, 
                     int pktbytes) {
  int pktnum = blkbytes / pktbytes;
  int slicesize = pktbytes / ecw;
  //cout << "fetchWorker::pktnum = " << pktnum << ", slicesize: " << slicesize << endl;

  struct timeval time1, time2;
  gettimeofday(&time1, NULL);

  if (loc == 0) {
      //cout << "Worker::fetchWorker generates zero bytes for " << keybase << endl;
      for (int i=0; i<pktnum; i++) {
          DataPacket* pkt = new DataPacket(slicesize);
          fetchQueue->push(pkt);
      }
  } else {
    redisReply* rReply;
    redisContext* fetchCtx = RedisUtil::createContext(loc);
    //cout << "fetchWorker::connect to " << RedisUtil::ip2Str(loc) << " for keybase " << keybase << endl;

    int replyid=0;
    for (int i=0; i<pktnum; i++) {
      //string key = keybase+":"+to_string(i);
      string key = keybase;
      redisAppendCommand(fetchCtx, "blpop %s 0", key.c_str());
    }
    
    struct timeval t1, t2;
    double t;
    for (int i=replyid; i<pktnum; i++) {
      //string key = keybase+":"+to_string(i);
      string key = keybase;
      gettimeofday(&t1, NULL);
      redisGetReply(fetchCtx, (void**)&rReply);
      gettimeofday(&t2, NULL);
      //if (i == 0) cout << "OECWorker::fetchWorker.fetch first t = " << RedisUtil::duration(t1, t2) << endl;
      char* content = rReply->element[1]->str;
      DataPacket* pkt = new DataPacket(content);
      int curDataLen = pkt->getDatalen();
      //cout << "fetch data len: " << curDataLen << endl;
      fetchQueue->push(pkt);
      freeReplyObject(rReply);
    }
    gettimeofday(&time2, NULL);
    cout << "Worker::fetchWorker.duration: " << DistUtil::duration(time1, time2) << " for " << keybase << endl;
    redisFree(fetchCtx);
  }
}

void Worker::fetchWorker2(unordered_map<int, BlockingQueue<DataPacket*>*> fetchMap,
                     string stripename,
                     unsigned int loc,
                     int ecw,
                     int blkbytes, 
                     int pktbytes) {
  int pktnum = blkbytes / pktbytes;
  int slicesize = pktbytes / ecw;
  //cout << "fetchWorker::pktnum = " << pktnum << ", slicesize: " << slicesize << endl;

  struct timeval time1, time2;
  gettimeofday(&time1, NULL);

  if (loc == 0) {
      for (int i=0; i<pktnum; i++) {
          for (auto item: fetchMap) {
              //cout << "Worker::fetchWorker generates zero bytes for " << stripename + ":" + to_string(item.first) << endl;
              DataPacket* pkt = new DataPacket(slicesize);
              item.second->push(pkt);
          }
      }
  } else {
    redisReply* rReply;
    redisContext* fetchCtx = RedisUtil::createContext(loc);
    for (auto item: fetchMap) {
        string key = stripename + ":" + to_string(item.first);
        //cout << "fetchWorker2::connect to " << RedisUtil::ip2Str(loc) << " for keybase " << key << endl;
    }

    int replyid=0;
    for (int i=0; i<pktnum; i++) {
      //string key = keybase+":"+to_string(i);
      
        for (auto item: fetchMap) {
            int cid = item.first;
            string key = stripename + ":" + to_string(cid);
            redisAppendCommand(fetchCtx, "blpop %s 0", key.c_str());
        }
    }
    
    for (int i=replyid; i<pktnum; i++) {
      //string key = keybase+":"+to_string(i);
        for (auto item: fetchMap) {
            redisGetReply(fetchCtx, (void**)&rReply);
            char* content = rReply->element[1]->str;
            DataPacket* pkt = new DataPacket(content);
            item.second->push(pkt);
            freeReplyObject(rReply);
        }
    }
    gettimeofday(&time2, NULL);
    cout << "Worker::fetchWorker.duration: " << DistUtil::duration(time1, time2) << " for ";
    // for (auto item: fetchMap) {
    //     cout << item.first << " ";
    // }
    cout << endl;
    redisFree(fetchCtx);
  }
}

void Worker::fetchWorker3(BlockingQueue<DataPacket*>* fetchqueue,
                     string stripename,
                     vector<int> cidlist,
                     unsigned int loc,
                     int ecw,
                     int blkbytes, 
                     int pktbytes) {
  int pktnum = blkbytes / pktbytes;
  int slicesize = pktbytes / ecw;
  //cout << "fetchWorker::pktnum = " << pktnum << ", slicesize: " << slicesize << endl;

  struct timeval time1, time2;
  gettimeofday(&time1, NULL);

  if (loc == 0) {
      for (int i=0; i<pktnum; i++) {
          for (auto cid: cidlist) {
              //cout << "Worker::fetchWorker generates zero bytes for " << stripename + ":" + to_string(item.first) << endl;
              DataPacket* pkt = new DataPacket(slicesize);
              fetchqueue->push(pkt);
          }
      }
  } else {
    redisReply* rReply;
    redisContext* fetchCtx = RedisUtil::createContext(loc);

    int replyid=0;
    for (int i=0; i<pktnum; i++) {
      //string key = keybase+":"+to_string(i);
      
        for (int tmpi=0; tmpi<cidlist.size(); tmpi++) {
            int cid = cidlist[tmpi] ;
            string key = stripename + ":" + to_string(cid);
            redisAppendCommand(fetchCtx, "blpop %s 0", key.c_str());
        }
    }
    
    for (int i=replyid; i<pktnum; i++) {
      //string key = keybase+":"+to_string(i);
        for (int tmpi=0; tmpi<cidlist.size(); tmpi++) {
            int cid = cidlist[tmpi];
            redisGetReply(fetchCtx, (void**)&rReply);
            char* content = rReply->element[1]->str;
            DataPacket* pkt = new DataPacket(content);
            fetchqueue->push(pkt);
            freeReplyObject(rReply);
        }
    }
    gettimeofday(&time2, NULL);
    cout << "Worker::fetchWorker.duration: " << DistUtil::duration(time1, time2) << " for ";
    // for (auto item: fetchMap) {
    //     cout << item.first << " ";
    // }
    cout << endl;
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
  gettimeofday(&time2, NULL);
  cout << "Worker::computeWorker.compute duration: " << DistUtil::duration(time1, time2) << endl;
}

void Worker::computeWorker2(unordered_map<int, BlockingQueue<DataPacket*>*> fetchMap,
        unordered_map<int, BlockingQueue<DataPacket*>*> cacheMap,
        vector<ComputeTask*> ctlist, 
        int ecw, int blkbytes, int pktbytes) {

  struct timeval time1, time2, time3;
  gettimeofday(&time1, NULL);

  int subpktbytes = pktbytes / ecw;
  int pktnum = blkbytes / pktbytes;

  //cout << "subpktbytes: " << subpktbytes << " , pktnum: " << pktnum << endl;
  //cout << "fetchmap.size: " << fetchMap.size() << ", cacheMap.size: " << cacheMap.size() << endl;
   
  for (int i=0; i<pktnum; i++) {
    unordered_map<int, char*> bufMap;
    unordered_map<int, DataPacket*> pktMap;

    // read from fetchmap
    for (auto item: fetchMap) {
        int curidx = item.first;
        //cout << "get " << curidx << endl;
        DataPacket* curpkt = fetchMap[curidx]->pop();
        pktMap.insert(make_pair(curidx, curpkt));
        bufMap.insert(make_pair(curidx, curpkt->getData()));

        //if (i == 0)
        //    cout << "prepare fetch cid: " << curidx << endl;
    }

    // prepare data pkt for writemap
    for (auto item: cacheMap) {
        int curidx = item.first;
        DataPacket* curpkt = new DataPacket(subpktbytes);
        pktMap.insert(make_pair(curidx, curpkt));
        bufMap.insert(make_pair(curidx, curpkt->getData()));

        //if (i == 0)
        //    cout << "prepare cache cid: " << curidx << endl;
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
    for (auto item: cacheMap) {
        int curidx = item.first;
        DataPacket* curpkt = pktMap[curidx];
        cacheMap[curidx]->push(curpkt);
        ignore.insert(make_pair(curidx, 1));
    }
    // free data packet
    for (auto item: fetchMap) {
        int curidx = item.first;
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
  gettimeofday(&time2, NULL);
  cout << "Worker::computeWorker2.compute duration: " << DistUtil::duration(time1, time2) << endl;
}

void Worker::computeWorker3(unordered_map<unsigned int, BlockingQueue<DataPacket*>*> fetchMap,
        unordered_map<unsigned int, vector<int>> cidmap,
        BlockingQueue<DataPacket*>* cachequeue,
        vector<int> cachelist,
        vector<ComputeTask*> ctlist, 
        int ecw, int blkbytes, int pktbytes) {

  struct timeval time1, time2, time3;
  gettimeofday(&time1, NULL);

  int subpktbytes = pktbytes / ecw;
  int pktnum = blkbytes / pktbytes;

  //cout << "subpktbytes: " << subpktbytes << " , pktnum: " << pktnum << endl;
  //cout << "fetchmap.size: " << fetchMap.size() << ", cacheMap.size: " << cacheMap.size() << endl;
  
  for (int i=0; i<pktnum; i++) {
    unordered_map<int, char*> bufMap;
    unordered_map<int, DataPacket*> pktMap;

    // read from fetchmap
    for (auto item: fetchMap) {
        unsigned int ip = item.first;
        BlockingQueue<DataPacket*>* queue = item.second;
        vector<int> cidlist = cidmap[ip];
        for (int tmpi=0; tmpi<cidlist.size(); tmpi++) {
            int curidx = cidlist[tmpi];
            DataPacket* curpkt = queue->pop();
            pktMap.insert(make_pair(curidx, curpkt));
            bufMap.insert(make_pair(curidx, curpkt->getData()));
        }
    }

    // prepare data pkt for cachequeue
    for (int tmpi=0; tmpi<cachelist.size(); tmpi++) {
        int curidx = cachelist[tmpi];
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
    for (int tmpi=0; tmpi<cachelist.size(); tmpi++) {
        int curidx = cachelist[tmpi];
        DataPacket* curpkt = pktMap[curidx];
        cachequeue->push(curpkt);
        ignore.insert(make_pair(curidx, 1));
    }
    // free data packet
    for (auto item: fetchMap) {
        unsigned int ip = item.first;
        vector<int> cidlist = cidmap[ip];
        for (auto curidx: cidlist) {
            DataPacket* curpkt = pktMap[curidx];
            delete curpkt;
            ignore.insert(make_pair(curidx, 1));
        }
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
  gettimeofday(&time2, NULL);
  cout << "Worker::computeWorker3.compute duration: " << DistUtil::duration(time1, time2) << endl;
}

void Worker::readAndCompute(AGCommand* agcmd) {
    
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);
    
    string blockname = agcmd->getBlockName();
    int blkbytes = agcmd->getBlkBytes();
    int pktbytes = agcmd->getPktBytes();
    vector<int> cidlist = agcmd->getIndices();
    int ecw = agcmd->getECW();
    string stripename = agcmd->getStripeName();
    int nCompute = agcmd->getNCompute();
    unordered_map<int, int> cid2refs = agcmd->getCid2Refs();
    int taskid = agcmd->getTaskid();
    
    //cout << "Worker::blockname = " << blockname << endl;
    //cout << "Worker::blkbytes = " << blkbytes << endl;
    //cout << "Worker::pktbytes = " << pktbytes << endl;
    //cout << "Worker::cidlist: ";
    //for (auto cid: cidlist)
    //    cout << cid << " ";
    //cout << endl;
    //cout << "Worker::ecw = " << ecw << endl;
    //cout << "Worker::stripename = " << stripename << endl;
    //cout << "Worker::nCompute = " << nCompute << endl;
    //cout << "Worker::cid2refs: ";
    //for (auto item: cid2refs) 
    //    cout << "(" << item.first << "," << item.second << ") ";
    //cout << endl;
    //cout << "Worker::taskid = " << taskid << endl;

    // read threads
    vector<int> pattern;
    vector<int> patternidx;
    for (int i=0; i<ecw; i++)
        pattern.push_back(0);
    for (int i=0; i<ecw; i++)
        patternidx.push_back(-1);
    for (auto item: cidlist) {
        int cid = item;
        int j = cid % ecw;
        pattern[j] = 1;
        patternidx[j] = cid;
    }
    // cout << "Worker::readDisk.pattern: ";
    // for (int i=0; i<ecw; i++)
    //   cout << pattern[i] << " ";
    // cout << endl;

    unordered_map<int, BlockingQueue<DataPacket*>*> readmap;
    for (int i=0; i<patternidx.size(); i++) {
        if (patternidx[i] == -1)
            continue;
        int cid = patternidx[i];
        BlockingQueue<DataPacket*>* readqueue = new BlockingQueue<DataPacket*>();
        readmap.insert(make_pair(cid, readqueue));
    }
    
    thread readThread = thread([=]{readWorker(readmap, blockname, ecw, pattern, patternidx, blkbytes, pktbytes);});

    redisContext* redisCtx = RedisUtil::createContext(_conf -> _localIp);
    redisReply* rReply;

    // read compute commands
    vector<ComputeTask*> computeTaskList;
    string rkey = stripename + ":task" + to_string(taskid) + ":compute";
    for (int i=0; i<nCompute; i++) {
        rReply = (redisReply*)redisCommand(redisCtx, "blpop %s 0", rkey.c_str());
        char* reqStr = rReply -> element[1] -> str;
        ComputeCommand* computeCmd = new ComputeCommand(reqStr);
        vector<int> srclist = computeCmd->getSrcList();
        vector<int> dstlist = computeCmd->getDstList();
        vector<vector<int>> coefs = computeCmd->getCoefs();

        ComputeTask* ct = new ComputeTask(srclist, dstlist, coefs);
        computeTaskList.push_back(ct);

        freeReplyObject(rReply);
        computeCmd->setCmd(NULL, 0);
        delete computeCmd;
    }

    // create cachemap
    unordered_map<int, BlockingQueue<DataPacket*>*> cachemap;
    for (auto item: cid2refs) {
        int cid = item.first;
        BlockingQueue<DataPacket*>* queue = new BlockingQueue<DataPacket*>();
        cachemap.insert(make_pair(cid, queue));
    }

    // create compute thread
    thread computeThread = thread([=]{computeWorker2(readmap, cachemap, computeTaskList, ecw, blkbytes, pktbytes);});

    // create cache thread
    thread cacheThread = thread([=]{cacheWorker2(cachemap, cid2refs, ecw, stripename, blkbytes, pktbytes);});

    readThread.join();
    computeThread.join();
    cacheThread.join();

    // free
    free(redisCtx);
    for (auto item: readmap) {
        delete item.second;
    }
    for (auto item: cachemap) {
        delete item.second;
    }
    for (auto item: computeTaskList){
        delete item;
    }
}

void Worker::readAndComputeWithOffset(AGCommand* agcmd) {
    
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);
    
    string blockname = agcmd->getBlockName();
    int blkbytes = agcmd->getBlkBytes();
    int pktbytes = agcmd->getPktBytes();
    vector<int> cidlist = agcmd->getIndices();
    int ecw = agcmd->getECW();
    string stripename = agcmd->getStripeName();
    int nCompute = agcmd->getNCompute();
    unordered_map<int, int> cid2refs = agcmd->getCid2Refs();
    int taskid = agcmd->getTaskid();
    int offset = agcmd->getOffset();
    
    //cout << "Worker::blockname = " << blockname << endl;
    //cout << "Worker::blkbytes = " << blkbytes << endl;
    //cout << "Worker::pktbytes = " << pktbytes << endl;
    //cout << "Worker::cidlist: ";
    //for (auto cid: cidlist)
    //    cout << cid << " ";
    //cout << endl;
    //cout << "Worker::ecw = " << ecw << endl;
    //cout << "Worker::stripename = " << stripename << endl;
    //cout << "Worker::nCompute = " << nCompute << endl;
    //cout << "Worker::cid2refs: ";
    //for (auto item: cid2refs) 
    //    cout << "(" << item.first << "," << item.second << ") ";
    //cout << endl;
    //cout << "Worker::taskid = " << taskid << endl;

    // read threads
    vector<int> pattern;
    vector<int> patternidx;
    for (int i=0; i<ecw; i++)
        pattern.push_back(0);
    for (int i=0; i<ecw; i++)
        patternidx.push_back(-1);
    for (auto item: cidlist) {
        int cid = item;
        int j = cid % ecw;
        pattern[j] = 1;
        patternidx[j] = cid;
    }
    cout << "Worker::readDisk.pattern: ";
    for (int i=0; i<ecw; i++)
      cout << pattern[i] << " ";
    cout << endl;

    unordered_map<int, BlockingQueue<DataPacket*>*> readmap;
    for (int i=0; i<patternidx.size(); i++) {
        if (patternidx[i] == -1)
            continue;
        int cid = patternidx[i];
        BlockingQueue<DataPacket*>* readqueue = new BlockingQueue<DataPacket*>();
        readmap.insert(make_pair(cid, readqueue));
    }
    
    thread readThread = thread([=]{readWorkerWithOffset(readmap, blockname, ecw, pattern, patternidx, blkbytes, pktbytes, offset);});

    redisContext* redisCtx = RedisUtil::createContext(_conf -> _localIp);
    redisReply* rReply;

    // read compute commands
    vector<ComputeTask*> computeTaskList;
    string rkey = stripename + ":task" + to_string(taskid) + ":compute";
    for (int i=0; i<nCompute; i++) {
        rReply = (redisReply*)redisCommand(redisCtx, "blpop %s 0", rkey.c_str());
        char* reqStr = rReply -> element[1] -> str;
        ComputeCommand* computeCmd = new ComputeCommand(reqStr);
        vector<int> srclist = computeCmd->getSrcList();
        vector<int> dstlist = computeCmd->getDstList();
        vector<vector<int>> coefs = computeCmd->getCoefs();

        ComputeTask* ct = new ComputeTask(srclist, dstlist, coefs);
        computeTaskList.push_back(ct);

        freeReplyObject(rReply);
        computeCmd->setCmd(NULL, 0);
        delete computeCmd;
    }

    // create cachemap
    unordered_map<int, BlockingQueue<DataPacket*>*> cachemap;
    for (auto item: cid2refs) {
        int cid = item.first;
        BlockingQueue<DataPacket*>* queue = new BlockingQueue<DataPacket*>();
        cachemap.insert(make_pair(cid, queue));
    }

    // create compute thread
    thread computeThread = thread([=]{computeWorker2(readmap, cachemap, computeTaskList, ecw, blkbytes, pktbytes);});

    // create cache thread
    thread cacheThread = thread([=]{cacheWorker2(cachemap, cid2refs, ecw, stripename, blkbytes, pktbytes);});

    readThread.join();
    computeThread.join();
    cacheThread.join();

    // free
    free(redisCtx);
    for (auto item: readmap) {
        delete item.second;
    }
    for (auto item: cachemap) {
        delete item.second;
    }
    for (auto item: computeTaskList){
        delete item;
    }
}
