#include "Coordinator.hh"

Coordinator::Coordinator(Config* conf, StripeStore* ss) : _conf(conf) {
  // create local context
  try {
    _localCtx = RedisUtil::createContext(_conf -> _localIp);
  } catch (int e) {
    // TODO: error handling
    cerr << "initializing redis context to " << " error" << endl;
  }
  _stripeStore = ss;
}

Coordinator::~Coordinator() {
  redisFree(_localCtx);
}

void Coordinator::doProcess() {
  redisReply* rReply;
  while (true) {
    cout << "Coordinator::doProcess" << endl;
    // will never stop looping
    rReply = (redisReply*)redisCommand(_localCtx, "blpop coor_request 0");
    if (rReply -> type == REDIS_REPLY_NIL) {
      cerr << "Coordinator::doProcess() get feed back empty queue " << endl;
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      cerr << "Coordinator::doProcess() get feed back ERROR happens " << endl;
    } else {
      cout << "Coordinator::doProcess() receive a request!" << endl;
      char* reqStr = rReply -> element[1] -> str;
      CoorCommand* coorCmd = new CoorCommand(reqStr);
      coorCmd->dump();
      int type = coorCmd->getType();
      switch (type) {
        case 0: repairBlock(coorCmd); break;
        case 1: repairNode(coorCmd); break;
        case 2: readBlock(coorCmd); break;
        default: break;
      }
      delete coorCmd;
    }
    // free reply object
    freeReplyObject(rReply);
  }
}

void Coordinator::repairBlock(CoorCommand* coorCmd) {
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);

    unsigned int clientIp = coorCmd->getClientIp();
    string blockName = coorCmd->getBlockName();
    string method = coorCmd->getMethod();

    // verify whether client is in clientnodes
    if (find(_conf->_clientIPs.begin(), _conf->_clientIPs.end(), clientIp) == _conf->_clientIPs.end()) {
        // we find that the request is sent from a non-client node
        // modify the client to a client node
        clientIp = _conf->_clientIPs[0];
        cout << "Coor::repairBlock.revise repair ip to " << RedisUtil::ip2Str(clientIp) << endl;
    }

    cout << "Coor::repairBlock.blockname: " << blockName << ", method: " << method << endl;

    if (method == "dist") {
        repairBlockDist1(blockName, clientIp, true, false);
    } else if (method == "conv") {
        repairBlockConv(blockName, clientIp, true, false);
    } else {
        cout << "ERROR::wrong method!" << endl;
    }
}

void Coordinator::repairBlockConv(string blockName, unsigned int clientip, bool enforceip, bool wait) {
    cout << "Coor::repairBlockConv:blockName: " << blockName << endl;
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);

    // 0. figure out the stripe that contains this block
    StripeMeta* stripemeta = _stripeStore->getStripeMetaFromBlockName(blockName);
    string stripename = stripemeta->getStripeName();
    long long blkbytes = stripemeta->getBlockBytes();
    int pktbytes = stripemeta->getPacketBytes();
    cout << "stripename: " << stripename << ", blkbytes: " << blkbytes << ", pktybtes: " << pktbytes << endl;

    // 1. the index of the failed block
    int repairBlockIdx = stripemeta->getBlockIndex(blockName);
    cout << "Coor::repairBlockConv.repairBlockIdx: " << repairBlockIdx << endl;

    // 2. prepare blocklist and loclist
    int ecn = stripemeta->getECN();
    int eck = stripemeta->getECK();
    int ecw = stripemeta->getECW();

    vector<string> blocklist = stripemeta->getBlockList();
    vector<unsigned int> loclist = stripemeta->getLocList();

    //cout << "Coor::repairBlock.blocklist: " << endl;
    //for (int i=0; i<ecn; i++) {
    //    cout << "  " << blocklist[i] << ": " << RedisUtil::ip2Str(loclist[i]) << endl;
    //}

    // 3. figure out repairloc
    if (enforceip) {
        loclist[repairBlockIdx] = clientip;
    }
    unsigned int repairLoc = loclist[repairBlockIdx];

    // 4. prepare availidx
    vector<int> availIndex;
    vector<int> toRepairIndex;
    for (int i=0; i<ecn; i++) {
        for (int j=0; j<ecw; j++) {
            int pktidx = i*ecw+j;
            if (i == repairBlockIdx)
                toRepairIndex.push_back(pktidx);
            else
                availIndex.push_back(pktidx);
        }
    }
    //cout << "avail: ";
    //for (auto item: availIndex)
    //    cout << item << " ";
    //cout << endl;

    // 5. construct ECDAG
    ECBase* ec = stripemeta->createECClass();
    ecw = ec->_w;
    _stripeStore->lock();
    ECDAG* ecdag = ec->Decode(availIndex, toRepairIndex);
    _stripeStore->unlock();
    ecdag->Concact(toRepairIndex);

    // 6. divide ecdag into ecunits
    ecdag->genECUnits();

    // 7. get data structures from ecdag
    unordered_map<int, ECNode*> ecNodeMap = ecdag->getECNodeMap();
    vector<int> ecHeaders = ecdag->getECHeaders();
    vector<int> ecLeaves = ecdag->getECLeaves();
    unordered_map<int, ECUnit*> ecunits = ecdag->getUnitMap();
    vector<int> ecUnitList = ecdag->getUnitList();

    cout << "Total nodes: " << ecNodeMap.size() << endl;
    cout << "Header nodes: " << ecHeaders.size() << endl;
    cout << "Leaf nodes: " << ecLeaves.size() << endl;

    int intermediate_num = ecNodeMap.size() - ecHeaders.size() - ecLeaves.size();
    cout << "Intermediate nodes: " << intermediate_num << endl;

    vector<ECTask*> tasklist;
    ecdag->genConvECTasks(tasklist, ecn, eck, ecw, blkbytes, pktbytes, stripename, blocklist, loclist, repairBlockIdx);

    gettimeofday(&time2, NULL);

    // 8. send out tasks
    //cout << "ECTasks: " << endl;
    for (int i=0; i<tasklist.size(); i++) {

        //if (i > 4)
        //    break;

        ECTask* task = tasklist[i];
        //cout << "  " << tasklist[i]->dumpStr() << endl;
        task->sendTask(i);
    }

    // wait for finish flag?
    if (wait) {
        redisContext* waitCtx = RedisUtil::createContext(repairLoc);
        string wkey = "writefinish:"+blockName;
        redisReply* fReply = (redisReply*)redisCommand(waitCtx, "blpop %s 0", wkey.c_str());
        freeReplyObject(fReply);
        redisFree(waitCtx);
        gettimeofday(&time3, NULL);
        //cout << "repairBlockConv:: prepair time: " << DistUtil::duration(time1, time2) << ", repair time: " << DistUtil::duration(time2, time3) << endl;
    }

    // delete
    delete ec;
    delete ecdag;
    for (int i=0; i<tasklist.size(); i++) {
        delete tasklist[i];
    }
}

void Coordinator::repairBlockListConv(vector<string> blocklist) {
    cout << "Coordinator::repairBlockListConv" << endl;
    for (int i=0; i<blocklist.size(); i++) {
        repairBlockConv(blocklist[i], 0, false, true);
    }
}

void Coordinator::repairBlockConv1(string blockName) {
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);

    cout << "Coor::repairBlockConv1.blockName: " << blockName << endl;

    // 0. figure out the stripe that contains this block
    StripeMeta* stripemeta = _stripeStore->getStripeMetaFromBlockName(blockName);
    string stripename = stripemeta->getStripeName();
    long long blkbytes = stripemeta->getBlockBytes();
    int pktbytes = stripemeta->getPacketBytes();
    cout << "stripename: " << stripename << ", blkbytes: " << blkbytes << ", pktybtes: " << pktbytes << endl;

    // 1. the index of the failed block
    int repairBlockIdx = stripemeta->getBlockIndex(blockName);
    cout << "Coor::repairBlock.repairBlockIdx: " << repairBlockIdx << endl;

    // 2. prepare avail and to repair
    int ecn = stripemeta->getECN();
    int eck = stripemeta->getECK();
    int ecw = stripemeta->getECW();

    vector<string> blocklist = stripemeta->getBlockList();
    vector<unsigned int> loclist = stripemeta->getLocList();

    // cout << "Coor::repairBlock.blocklist: " << endl;
    // for (int i=0; i<ecn; i++) {
    //     cout << "  " << blocklist[i] << ": " << RedisUtil::ip2Str(loclist[i]) << endl;
    // }

    // 3. now we repair at the same location
    unsigned int repairLoc = loclist[repairBlockIdx];

    // 4. prepare availidx
    vector<int> availIndex;
    vector<int> toRepairIndex;
    for (int i=0; i<ecn; i++) {
        for (int j=0; j<ecw; j++) {
            int pktidx = i*ecw+j;
            if (i == repairBlockIdx)
                toRepairIndex.push_back(pktidx);
            else
                availIndex.push_back(pktidx);
        }
    }

    // 5. construct ECDAG
    ECBase* ec = stripemeta->createECClass();
    ecw = ec->_w;
    ECDAG* ecdag = ec->Decode(availIndex, toRepairIndex);
    ecdag->Concact(toRepairIndex);

    // 6. divide ecdag into ecunits
    ecdag->genECUnits();

    // 7. get data structures from ecdag
    unordered_map<int, ECNode*> ecNodeMap = ecdag->getECNodeMap();
    vector<int> ecHeaders = ecdag->getECHeaders();
    vector<int> ecLeaves = ecdag->getECLeaves();
    unordered_map<int, ECUnit*> ecunits = ecdag->getUnitMap();
    vector<int> ecUnitList = ecdag->getUnitList();

    cout << "Total nodes: " << ecNodeMap.size() << endl;
    cout << "Header nodes: " << ecHeaders.size() << endl;
    cout << "Leaf nodes: " << ecLeaves.size() << endl;

    int intermediate_num = ecNodeMap.size() - ecHeaders.size() - ecLeaves.size();
    cout << "Intermediate nodes: " << intermediate_num << endl;

    // 8. initialize colors
    // we use idx from 0 to n-1 represents the n candidate colors
    // for leave vertices, if it is stored in block [i], then we initialize the color to i.
    // for header, we use the repairBlockIdx for its color
    unordered_map<int, int> sidx2bidx;
    int shortnum = 0;
    for (auto sidx: ecLeaves) {
        int bidx = sidx / ecw;
        if (bidx < ecn )
            sidx2bidx.insert(make_pair(sidx, bidx));
        else {
            shortnum++;
            sidx2bidx.insert(make_pair(sidx, -1));  // virtual blocks
        }
    }
    for (auto sidx: ecHeaders) {
        sidx2bidx.insert(make_pair(sidx, repairBlockIdx));
    }

//    cout << "sidx2bidx: " << endl;
//    for (auto item: sidx2bidx) {
//        int cidx = item.first;
//        int bidx = item.second;
//        cout << "  cidx: " << cidx << ", bidx: " << bidx << endl;
//    }
//
//    cout << "num shortening: " << shortnum << endl;

    // 9. for intermediate vertices
    // note that virtual vertices for shortening are not counted as intermediate vertices
    vector<int> itm_idx;
    vector<int> candidates;
    for (auto item: ecNodeMap) {
        int sidx = item.first;
        if (find(ecHeaders.begin(), ecHeaders.end(), sidx) != ecHeaders.end())
            continue;
        if (find(ecLeaves.begin(), ecLeaves.end(), sidx) != ecLeaves.end())
            continue;
        itm_idx.push_back(sidx);
        //sidx2bidx.insert(make_pair(sidx, -1));
    }

    for (int i=0; i<ecn; i++)
        candidates.push_back(i);
    sort(itm_idx.begin(), itm_idx.end());

    // 10. hardcode conv coloring
    vector<int> itm_coloring;
    for (int i=0; i<itm_idx.size(); i++)
        itm_coloring.push_back(repairBlockIdx);

//    // 10. read the tradeoff point and calculate maxload and bdwt
//    string tpentry = stripemeta->getCodeName() + "_" + to_string(ecn) + "_" + to_string(eck) + "_" + to_string(ecw);
//    cout << "tpentry: " << tpentry << endl;
//    TradeoffPoints* tp = _stripeStore->getTradeoffPoints(tpentry);
//    vector<int> itm_coloring = tp->getColoringByIdx(repairBlockIdx);
//
//    cout << "itm coloring: " << endl;
//    for (int i=0; i<itm_idx.size(); i++) {
//        cout << "  idx: " << itm_idx[i] << ", color: " << itm_coloring[i] << endl;
//    }
//    cout << endl;
//
//    cout << "itm.size: " << itm_idx.size() << endl;

    int bdwt, maxload;
    stat(sidx2bidx, itm_coloring, itm_idx, ecdag, &bdwt, &maxload);
    cout << "maxload: " << maxload << ", bdwt: " << bdwt << endl;

    // 11. generate coloring results
    unordered_map<int, unsigned int> coloring_res;
    for (auto item: sidx2bidx) {
        int idx = item.first;
        int color = item.second;
        unsigned int ip;
        if (color < 0)
            ip = 0;
        else
            ip = loclist[color];
        coloring_res.insert(make_pair(idx, ip));
    }
    for (int i=0; i<itm_idx.size(); i++) {
        int idx = itm_idx[i];
        int color = itm_coloring[i];
        unsigned int ip = loclist[color];
        coloring_res.insert(make_pair(idx, ip));
    }

    // cout << "coloring res: " << endl;
    // for (auto item: coloring_res) {
    //     cout << "  idx: " << item.first << ", ip: " << RedisUtil::ip2Str(item.second) << endl;
    // }

    // 12. generate ectasks by ECClusters
    cout << "blkbytes: " << blkbytes << ", pktbytes: " << pktbytes << endl;
    vector<ECTask*> tasklist;
    ecdag->genECTasksByECClusters(tasklist, ecn, eck, ecw, blkbytes, pktbytes, stripename, blocklist, coloring_res);
    //cout << "tasklist: " << endl;
    //for (int i=0; i<tasklist.size(); i++) {
    //    cout << "  " << tasklist[i]->dumpStr() << endl;
    //}

    // 13. generate agcommands
    cout << "AGCommands: " << endl;
    vector<AGCommand*> cmdlist;
    int debug = 0;
    for (auto task: tasklist) {
      AGCommand* agcmd = task->genAGCommand();
      cmdlist.push_back(agcmd);
      //cout << "  " << agcmd->dumpStr() << endl; 
      //cout << "  " << agcmd->getCmdLen() << endl;

      debug++;
    }

    gettimeofday(&time2, NULL);

    // 14. send out commands
    vector<char*> todelete;
    redisContext* distCtx = RedisUtil::createContext(_conf->_coorIp);
    
    redisAppendCommand(distCtx, "MULTI");
    for (auto agcmd: cmdlist) {
      unsigned int ip = agcmd->getSendIp();
      ip = htonl(ip);
      char* cmdstr = agcmd->getCmd();
      int cmLen = agcmd->getCmdLen();
      char* todist = (char*)calloc(cmLen + 4, sizeof(char));
      memcpy(todist, (char*)&ip, 4);
      memcpy(todist+4, cmdstr, cmLen); 
      todelete.push_back(todist);
      redisAppendCommand(distCtx, "RPUSH dist_request %b", todist, cmLen+4);
    }
    redisAppendCommand(distCtx, "EXEC");
    
    redisReply* distReply;
    redisGetReply(distCtx, (void **)&distReply);
    freeReplyObject(distReply);
    for (auto item: todelete) {
      redisGetReply(distCtx, (void **)&distReply);
      freeReplyObject(distReply);
    }
    redisGetReply(distCtx, (void **)&distReply);
    freeReplyObject(distReply);
    redisFree(distCtx);
 
    // wait for finish flag?
    redisContext* waitCtx = RedisUtil::createContext(repairLoc);
    string wkey = "writefinish:"+blockName;
    redisReply* fReply = (redisReply*)redisCommand(waitCtx, "blpop %s 0", wkey.c_str());
    freeReplyObject(fReply);
    redisFree(waitCtx);
    gettimeofday(&time3, NULL);
    cout << "repairBlockConv1:: prepair time: " << DistUtil::duration(time1, time2) << ", repair time: " << DistUtil::duration(time2, time3) << endl;
 
    // delete
    delete ec;
    delete ecdag;
    for (auto item: cmdlist) delete item;
    for (auto item: todelete) free(item);
}

void Coordinator::repairBlockDist(string blockName) {
    cout << "Coor::repairBlockDist.blockName: " << blockName << endl;
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);

    // 0. figure out the stripe that contains this block
    StripeMeta* stripemeta = _stripeStore->getStripeMetaFromBlockName(blockName);
    string stripename = stripemeta->getStripeName();
    long long blkbytes = stripemeta->getBlockBytes();
    int pktbytes = stripemeta->getPacketBytes();
    cout << "stripename: " << stripename << ", blkbytes: " << blkbytes << ", pktybtes: " << pktbytes << endl;

    // 1. the index of the failed block
    int repairBlockIdx = stripemeta->getBlockIndex(blockName);
    cout << "Coor::repairBlock.repairBlockIdx: " << repairBlockIdx << endl;

    // 2. prepare avail and to repair
    int ecn = stripemeta->getECN();
    int eck = stripemeta->getECK();
    int ecw = stripemeta->getECW();

    vector<string> blocklist = stripemeta->getBlockList();
    vector<unsigned int> loclist = stripemeta->getLocList();

    //cout << "Coor::repairBlock.blocklist: " << endl;
    //for (int i=0; i<ecn; i++) {
    //    cout << "  " << blocklist[i] << ": " << RedisUtil::ip2Str(loclist[i]) << endl;
    //}

    // 3. now we repair at the same location
    unsigned int repairLoc = loclist[repairBlockIdx];

    // 4. prepare availidx
    vector<int> availIndex;
    vector<int> toRepairIndex;
    for (int i=0; i<ecn; i++) {
        for (int j=0; j<ecw; j++) {
            int pktidx = i*ecw+j;
            if (i == repairBlockIdx)
                toRepairIndex.push_back(pktidx);
            else
                availIndex.push_back(pktidx);
        }
    }

    // 5. construct ECDAG
    ECBase* ec = stripemeta->createECClass();
    ecw = ec->_w;
    ECDAG* ecdag = ec->Decode(availIndex, toRepairIndex);
    ecdag->Concact(toRepairIndex);

    // 6. divide ecdag into ecunits
    ecdag->genECUnits();

    // 7. get data structures from ecdag
    unordered_map<int, ECNode*> ecNodeMap = ecdag->getECNodeMap();
    vector<int> ecHeaders = ecdag->getECHeaders();
    vector<int> ecLeaves = ecdag->getECLeaves();
    unordered_map<int, ECUnit*> ecunits = ecdag->getUnitMap();
    vector<int> ecUnitList = ecdag->getUnitList();

    cout << "Total nodes: " << ecNodeMap.size() << endl;
    cout << "Header nodes: " << ecHeaders.size() << endl;
    cout << "Leaf nodes: " << ecLeaves.size() << endl;

    int intermediate_num = ecNodeMap.size() - ecHeaders.size() - ecLeaves.size();
    cout << "Intermediate nodes: " << intermediate_num << endl;

    // 8. initialize colors
    // we use idx from 0 to n-1 represents the n candidate colors
    // for leave vertices, if it is stored in block [i], then we initialize the color to i.
    // for header, we use the repairBlockIdx for its color
    unordered_map<int, int> sidx2bidx;
    int shortnum = 0;
    for (auto sidx: ecLeaves) {
        int bidx = sidx / ecw;
        if (bidx < ecn )
            sidx2bidx.insert(make_pair(sidx, bidx));
        else {
            shortnum++;
            sidx2bidx.insert(make_pair(sidx, -1));  // virtual blocks
        }
    }
    for (auto sidx: ecHeaders) {
        sidx2bidx.insert(make_pair(sidx, repairBlockIdx));
    }

//    cout << "sidx2bidx: " << endl;
//    for (auto item: sidx2bidx) {
//        int cidx = item.first;
//        int bidx = item.second;
//        cout << "  cidx: " << cidx << ", bidx: " << bidx << endl;
//    }
//
//    cout << "num shortening: " << shortnum << endl;

    // 9. for intermediate vertices
    // note that virtual vertices for shortening are not counted as intermediate vertices
    vector<int> itm_idx;
    vector<int> candidates;
    for (auto item: ecNodeMap) {
        int sidx = item.first;
        if (find(ecHeaders.begin(), ecHeaders.end(), sidx) != ecHeaders.end())
            continue;
        if (find(ecLeaves.begin(), ecLeaves.end(), sidx) != ecLeaves.end())
            continue;
        itm_idx.push_back(sidx);
        //sidx2bidx.insert(make_pair(sidx, -1));
    }

    for (int i=0; i<ecn; i++)
        candidates.push_back(i);
    sort(itm_idx.begin(), itm_idx.end());

    // 10. read the tradeoff point and calculate maxload and bdwt
    string tpentry = stripemeta->getCodeName() + "_" + to_string(ecn) + "_" + to_string(eck) + "_" + to_string(ecw);
    cout << "tpentry: " << tpentry << endl;
    TradeoffPoints* tp = _stripeStore->getTradeoffPoints(tpentry);
    vector<int> itm_coloring = tp->getColoringByIdx(repairBlockIdx);

//    cout << "itm coloring: " << endl;
//    for (int i=0; i<itm_idx.size(); i++) {
//        cout << "  idx: " << itm_idx[i] << ", color: " << itm_coloring[i] << endl;
//    }
//    cout << endl;
//
//    cout << "itm.size: " << itm_idx.size() << endl;

    int bdwt, maxload;
    stat(sidx2bidx, itm_coloring, itm_idx, ecdag, &bdwt, &maxload);
    cout << "maxload: " << maxload << ", bdwt: " << bdwt << endl;

    // 11. generate coloring results
    unordered_map<int, unsigned int> coloring_res;
    for (auto item: sidx2bidx) {
        int idx = item.first;
        int color = item.second;
        unsigned int ip;
        if (color < 0)
            ip = 0;
        else
            ip = loclist[color];
        coloring_res.insert(make_pair(idx, ip));
    }
    for (int i=0; i<itm_idx.size(); i++) {
        int idx = itm_idx[i];
        int color = itm_coloring[i];
        unsigned int ip = loclist[color];
        coloring_res.insert(make_pair(idx, ip));
    }

    // cout << "coloring res: " << endl;
    // for (auto item: coloring_res) {
    //     cout << "  idx: " << item.first << ", ip: " << RedisUtil::ip2Str(item.second) << endl;
    // }

    // 12. generate ectasks by ECClusters
    cout << "blkbytes: " << blkbytes << ", pktbytes: " << pktbytes << endl;
    vector<ECTask*> tasklist;
    ecdag->genDistECTasks(tasklist, ecn, eck, ecw, blkbytes, pktbytes, stripename, blocklist, coloring_res);

    gettimeofday(&time2, NULL);
    // 8. send out tasks
    cout << "ECTasks: " << tasklist.size() << endl;
    for (int i=0; i<tasklist.size(); i++) {

        //if (i > 347)
        //    break;

        ECTask* task = tasklist[i];
        //cout << "  " << tasklist[i]->dumpStr() << endl;
        task->sendTask(i);
    }

    // wait for finish flag?
    redisContext* waitCtx = RedisUtil::createContext(repairLoc);
    string wkey = "writefinish:"+blockName;
    redisReply* fReply = (redisReply*)redisCommand(waitCtx, "blpop %s 0", wkey.c_str());
    freeReplyObject(fReply);
    redisFree(waitCtx);
    gettimeofday(&time3, NULL);
    cout << "repairBlockConv:: prepair time: " << DistUtil::duration(time1, time2) << ", repair time: " << DistUtil::duration(time2, time3) << endl;
 
    // delete
    delete ec;
    delete ecdag;
    for (int i=0; i<tasklist.size(); i++) {
        delete tasklist[i];
    }
}

void Coordinator::repairBlockDist1(string blockName, unsigned int clientip, bool enforceip, bool wait) {
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);

    cout << "Coor::repairBlock.blockName: " << blockName << endl;

    // 0. figure out the stripe that contains this block
    StripeMeta* stripemeta = _stripeStore->getStripeMetaFromBlockName(blockName);
    string stripename = stripemeta->getStripeName();
    long long blkbytes = stripemeta->getBlockBytes();
    int pktbytes = stripemeta->getPacketBytes();
    cout << "stripename: " << stripename << ", blkbytes: " << blkbytes << ", pktybtes: " << pktbytes << endl;

    // 1. the index of the failed block
    int repairBlockIdx = stripemeta->getBlockIndex(blockName);
    cout << "Coor::repairBlock.repairBlockIdx: " << repairBlockIdx << endl;

    // 2. prepare avail and to repair
    int ecn = stripemeta->getECN();
    int eck = stripemeta->getECK();
    int ecw = stripemeta->getECW();

    vector<string> blocklist = stripemeta->getBlockList();
    vector<unsigned int> loclist = stripemeta->getLocList();

    // cout << "Coor::repairBlock.blocklist: " << endl;
    // for (int i=0; i<ecn; i++) {
    //     cout << "  " << blocklist[i] << ": " << RedisUtil::ip2Str(loclist[i]) << endl;
    // }

    // // 3. now we repair at the same location
    // unsigned int repairLoc = loclist[repairBlockIdx];

    // 3. if enforceip, enforce clientip; otherwise choose the default
    unsigned int repairLoc;
    if (enforceip) {
        repairLoc = clientip;
        loclist[repairBlockIdx] = clientip;
    } else {
        repairLoc = loclist[repairBlockIdx];
    }

    // 4. prepare availidx
    vector<int> availIndex;
    vector<int> toRepairIndex;
    for (int i=0; i<ecn; i++) {
        for (int j=0; j<ecw; j++) {
            int pktidx = i*ecw+j;
            if (i == repairBlockIdx)
                toRepairIndex.push_back(pktidx);
            else
                availIndex.push_back(pktidx);
        }
    }

    // 5. construct ECDAG
    ECBase* ec = stripemeta->createECClass();
    ecw = ec->_w;
    ECDAG* ecdag = ec->Decode(availIndex, toRepairIndex);
    ecdag->Concact(toRepairIndex);

    // 6. divide ecdag into ecunits
    ecdag->genECUnits();

    // 7. get data structures from ecdag
    unordered_map<int, ECNode*> ecNodeMap = ecdag->getECNodeMap();
    vector<int> ecHeaders = ecdag->getECHeaders();
    vector<int> ecLeaves = ecdag->getECLeaves();
    unordered_map<int, ECUnit*> ecunits = ecdag->getUnitMap();
    vector<int> ecUnitList = ecdag->getUnitList();

    cout << "Total nodes: " << ecNodeMap.size() << endl;
    cout << "Header nodes: " << ecHeaders.size() << endl;
    cout << "Leaf nodes: " << ecLeaves.size() << endl;

    int intermediate_num = ecNodeMap.size() - ecHeaders.size() - ecLeaves.size();
    cout << "Intermediate nodes: " << intermediate_num << endl;

    // 8. initialize colors
    // we use idx from 0 to n-1 represents the n candidate colors
    // for leave vertices, if it is stored in block [i], then we initialize the color to i.
    // for header, we use the repairBlockIdx for its color
    unordered_map<int, int> sidx2bidx;
    int shortnum = 0;
    for (auto sidx: ecLeaves) {
        int bidx = sidx / ecw;
        if (bidx < ecn )
            sidx2bidx.insert(make_pair(sidx, bidx));
        else {
            shortnum++;
            sidx2bidx.insert(make_pair(sidx, -1));  // virtual blocks
        }
    }
    for (auto sidx: ecHeaders) {
        sidx2bidx.insert(make_pair(sidx, repairBlockIdx));
    }

//    cout << "sidx2bidx: " << endl;
//    for (auto item: sidx2bidx) {
//        int cidx = item.first;
//        int bidx = item.second;
//        cout << "  cidx: " << cidx << ", bidx: " << bidx << endl;
//    }
//
//    cout << "num shortening: " << shortnum << endl;

    // 9. for intermediate vertices
    // note that virtual vertices for shortening are not counted as intermediate vertices
    vector<int> itm_idx;
    vector<int> candidates;
    for (auto item: ecNodeMap) {
        int sidx = item.first;
        if (find(ecHeaders.begin(), ecHeaders.end(), sidx) != ecHeaders.end())
            continue;
        if (find(ecLeaves.begin(), ecLeaves.end(), sidx) != ecLeaves.end())
            continue;
        itm_idx.push_back(sidx);
        //sidx2bidx.insert(make_pair(sidx, -1));
    }

    for (int i=0; i<ecn; i++)
        candidates.push_back(i);
    sort(itm_idx.begin(), itm_idx.end());

    // 10. read the tradeoff point and calculate maxload and bdwt
    string tpentry = stripemeta->getCodeName() + "_" + to_string(ecn) + "_" + to_string(eck) + "_" + to_string(ecw);
    cout << "tpentry: " << tpentry << endl;
    TradeoffPoints* tp = _stripeStore->getTradeoffPoints(tpentry);
    vector<int> itm_coloring = tp->getColoringByIdx(repairBlockIdx);

    //cout << "itm coloring: " << endl;
    //for (int i=0; i<itm_idx.size(); i++) {
    //    cout << "  idx: " << itm_idx[i] << ", color: " << itm_coloring[i] << endl;
    //}
    //cout << endl;

    //cout << "itm.size: " << itm_idx.size() << endl;

    int bdwt, maxload;
    stat(sidx2bidx, itm_coloring, itm_idx, ecdag, &bdwt, &maxload);
    cout << "maxload: " << maxload << ", bdwt: " << bdwt << endl;

    // 11. generate coloring results
    unordered_map<int, unsigned int> coloring_res;
    for (auto item: sidx2bidx) {
        int idx = item.first;
        int color = item.second;
        unsigned int ip;
        if (color < 0)
            ip = 0;
        else
            ip = loclist[color];
        coloring_res.insert(make_pair(idx, ip));
    }
    for (int i=0; i<itm_idx.size(); i++) {
        int idx = itm_idx[i];
        int color = itm_coloring[i];
        unsigned int ip = loclist[color];
        coloring_res.insert(make_pair(idx, ip));
    }

    //cout << "coloring res: " << endl;
    //for (auto item: coloring_res) {
    //    cout << "  idx: " << item.first << ", ip: " << RedisUtil::ip2Str(item.second) << endl;
    //}

    // 12. generate ectasks by ECClusters
    cout << "blkbytes: " << blkbytes << ", pktbytes: " << pktbytes << endl;
    vector<ECTask*> tasklist;
    ecdag->genECTasksByECClusters(tasklist, ecn, eck, ecw, blkbytes, pktbytes, stripename, blocklist, coloring_res);
    //cout << "tasklist: " << endl;
    //for (int i=0; i<tasklist.size(); i++) {
    //    cout << "  " << tasklist[i]->dumpStr() << endl;
    //}

    // 13. generate agcommands
    cout << "AGCommands: " << endl;
    vector<AGCommand*> cmdlist;
    int debug = 0;
    for (auto task: tasklist) {
      //cout << "debug: " << debug << endl;
      AGCommand* agcmd = task->genAGCommand();
      cmdlist.push_back(agcmd);
      //cout << "  " << agcmd->dumpStr() << endl; 
      //cout << "  " << agcmd->getCmdLen() << endl;

      debug++;
    }

    gettimeofday(&time2, NULL);

    // 14. send out commands
    vector<char*> todelete;
    redisContext* distCtx = RedisUtil::createContext(_conf->_coorIp);

    debug=0;
    redisAppendCommand(distCtx, "MULTI");
    for (auto agcmd: cmdlist) {
      unsigned int ip = agcmd->getSendIp();
      ip = htonl(ip);
      char* cmdstr = agcmd->getCmd();
      int cmLen = agcmd->getCmdLen();
      char* todist = (char*)calloc(cmLen + 4, sizeof(char));
      memcpy(todist, (char*)&ip, 4);
      memcpy(todist+4, cmdstr, cmLen); 
      todelete.push_back(todist);
      redisAppendCommand(distCtx, "RPUSH dist_request %b", todist, cmLen+4);

      debug++;
    }
    redisAppendCommand(distCtx, "EXEC");
    
    redisReply* distReply;
    redisGetReply(distCtx, (void **)&distReply);
    freeReplyObject(distReply);
    for (auto item: todelete) {
      redisGetReply(distCtx, (void **)&distReply);
      freeReplyObject(distReply);
    }
    redisGetReply(distCtx, (void **)&distReply);
    freeReplyObject(distReply);
    redisFree(distCtx);
 
    // wait for finish flag?
    if (wait) {
        redisContext* waitCtx = RedisUtil::createContext(repairLoc);
        string wkey = "writefinish:"+blockName;
        redisReply* fReply = (redisReply*)redisCommand(waitCtx, "blpop %s 0", wkey.c_str());
        freeReplyObject(fReply);
        redisFree(waitCtx);
        gettimeofday(&time3, NULL);
        //cout << "repairBlockDist1:: prepair time: " << DistUtil::duration(time1, time2) << ", repair time: " << DistUtil::duration(time2, time3) << endl;
    }
 
    // delete
    delete ec;
    delete ecdag;
    for (auto item: cmdlist) delete item;
    for (auto item: todelete) free(item);
}

void Coordinator::repairBlockListDist1(vector<string> blocklist) {
    cout << "Coordinator::repairBlockListDist1" << endl;
    for (int i=0; i<blocklist.size(); i++) {
        repairBlockDist1(blocklist[i], 0, false, true);
    }
}

void Coordinator::stat(unordered_map<int, int> sidx2ip,
        vector<int> curres, vector<int> itm_idx,
        ECDAG* ecdag, int* bdwt, int* maxload) {
    unordered_map<int, int> coloring_res;
    for (auto item: sidx2ip) {
        coloring_res.insert(make_pair(item.first, item.second));
    }
    for (int ii=0; ii<curres.size(); ii++) {
        int idx = itm_idx[ii];
        int color = curres[ii];
        coloring_res[idx] = color;
    }
    // gen ECClusters
    ecdag->clearECCluster();
    ecdag->genECCluster(coloring_res, _conf->_clusterSize);

    // gen stat
    unordered_map<int, int> inmap;
    unordered_map<int, int> outmap;
    ecdag->genStat(coloring_res, inmap, outmap);
    
    int bw=0, max=0;
    for (auto item: inmap) {
        bw+= item.second;
        if (item.second > max)
            max = item.second;
    }
    for (auto item: outmap) {
        if (item.second > max)
            max = item.second;
    }
    *bdwt = bw;
    *maxload = max;
}

void Coordinator::repairNode(CoorCommand* coorCmd) {

    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);

    unsigned int clientIp = coorCmd->getClientIp();
    unsigned int nodeip = coorCmd->getNodeIp();
    string code = coorCmd->getCode();
    string method = coorCmd->getMethod();

    cout << "Coor::repairNode.node: " << RedisUtil::ip2Str(nodeip) << ", code: " << code << ", method: " << method << endl;

    // 0. figure out blocks of code in nodeip
    unordered_map<string, StripeMeta*> blk2meta = _stripeStore->getBlock2StripeMeta(nodeip, code);

    // 1. allocate a random ip for each block to repair
    for (auto item: blk2meta) {
        string blk = item.first;
        StripeMeta* meta = item.second;
        // whether directly update in meta
        meta->updateLocForBlock(blk, _conf->_agentsIPs);
    }

    if (method == "dist") {
        repairNodeDist(nodeip, code, blk2meta);
    } else if (method == "conv") {
        repairNodeConv(nodeip, code, blk2meta);
    } else {
        cout << "ERROR::wrong method!" << endl;
    }
}

void Coordinator::repairNodeDist(unsigned int nodeip, string code, unordered_map<string, StripeMeta*> blk2meta) {
    cout << "Coordinator::repairNodeDist!" << endl;

    int rpthreads = _conf->_rpThreads;
    
    // 0. devide all the blocks into groups
    vector<vector<string>> rpgroups;
    for (int i=0; i<rpthreads; i++) {
        vector<string> tmplist;
        rpgroups.push_back(tmplist);
    }

    // xiaolu add 0905
    vector<string> blklist;
    for (auto item: blk2meta) {
        blklist.push_back(item.first);
    }
    srand(unsigned(time(NULL)));
    std::random_shuffle(blklist.begin(), blklist.end());

    for (int i=0; i<blklist.size(); i++) {
        int idx = i%rpthreads;
        rpgroups[idx].push_back(blklist[i]);
    }
    // xiaolu add ends

    // // xiaolu comment start
    // int idx=0;
    // for (auto item: blk2meta) {
    //     string blk = item.first;
    //     rpgroups[idx].push_back(blk);
    //     idx=(idx+1)%rpthreads;
    // }
    // // xiaolu comment end

    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);
    // 1. create threads
    vector<thread> repairThreads = vector<thread>(rpthreads);
    for (int i=0; i<rpthreads; i++) {
        repairThreads[i] = thread([=]{repairBlockListDist1(rpgroups[i]);});
    }

    // 2. join
    for (int i=0; i<rpthreads; i++) {
        repairThreads[i].join();
    }
    gettimeofday(&time2, NULL);
    cout << "Coordinator::repairNodeDist.fullnode recovery duration = " << DistUtil::duration(time1, time2) << endl;
}

void Coordinator::repairNodeConv(unsigned int nodeip, string code, unordered_map<string, StripeMeta*> blk2meta) {
    cout << "Coordinator::repairNodeConv!" << endl;

    int rpthreads = _conf->_rpThreads;
    
    // 0. devide all the blocks into groups
    vector<vector<string>> rpgroups;
    for (int i=0; i<rpthreads; i++) {
        vector<string> tmplist;
        rpgroups.push_back(tmplist);
    }

    // xiaolu add 0905
    vector<string> blklist;
    for (auto item: blk2meta) {
        blklist.push_back(item.first);
    }
    srand(unsigned(time(NULL)));
    std::random_shuffle(blklist.begin(), blklist.end());

    for (int i=0; i<blklist.size(); i++) {
        int idx = i%rpthreads;
        rpgroups[idx].push_back(blklist[i]);
    }
    // xiaolu add ends

    // xiaolu comment 0905
    // int idx=0;
    // for (auto item: blk2meta) {
    //     string blk = item.first;
    //     rpgroups[idx].push_back(blk);
    //     idx=(idx+1)%rpthreads;
    // }
    // xiaolu comment end

    for (int i=0; i<rpgroups.size(); i++) {
        cout << "group " << i << ": ";
        for (int j=0; j<rpgroups[i].size(); j++)
            cout << rpgroups[i][j] << " ";
        cout << endl;
    }

    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);
    // 1. create threads
    vector<thread> repairThreads = vector<thread>(rpthreads);
    for (int i=0; i<rpthreads; i++) {
        repairThreads[i] = thread([=]{repairBlockListConv(rpgroups[i]);});
    }

    // 2. join
    for (int i=0; i<rpthreads; i++) {
        repairThreads[i].join();
    }
    gettimeofday(&time2, NULL);
    cout << "Coordinator::repairNodeConv.fullnode recovery duration = " << DistUtil::duration(time1, time2) << endl;
}

void Coordinator::readBlock(CoorCommand* coorCmd) {
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);

    unsigned int clientIp = coorCmd->getClientIp();
    string blockName = coorCmd->getBlockName();
    int offset = coorCmd->getOffset();
    int length = coorCmd->getLength();
    string method = coorCmd->getMethod();

    // verify whether client is in clientnodes
    if (find(_conf->_clientIPs.begin(), _conf->_clientIPs.end(), clientIp) == _conf->_clientIPs.end()) {
        // we find that the request is sent from a non-client node
        // modify the client to a client node
        clientIp = _conf->_clientIPs[0];
        cout << "Coor::repairBlock.revise repair ip to " << RedisUtil::ip2Str(clientIp) << endl;
    }
    cout << "Coor::readBlock.blockname: " << blockName << ", offset: " << offset << ", length: " << length << ", method: " << method << endl;

    if (method == "dist") {
//        repairBlockDist1(blockName, clientIp, true);
    } else if (method == "conv") {
        readBlockConv(blockName, clientIp, true, offset, length);
    } else {
        cout << "ERROR::wrong method!" << endl;
    }
}

void Coordinator::readBlockConv(string blockName, unsigned int clientip, bool enforceip, int offset, int length) {
    cout << "Coor::readBlockConv:blockName: " << blockName << endl;
    struct timeval time1, time2, time3;
    gettimeofday(&time1, NULL);

    // 0. figure out the stripe that contains this block
    StripeMeta* stripemeta = _stripeStore->getStripeMetaFromBlockName(blockName);
    string stripename = stripemeta->getStripeName();
    long long blkbytes = stripemeta->getBlockBytes();
    int pktbytes = stripemeta->getPacketBytes();

    // 1. figure out substripe info
    int substripe_offset = offset / pktbytes * pktbytes;
    int substripe_blockbytes;
    if (length % pktbytes == 0)
        substripe_blockbytes = length / pktbytes;
    else
        substripe_blockbytes = (length / pktbytes + 1) * pktbytes;
    cout << "stripename: " << stripename << ", substripe_offset: " << substripe_offset << ", substripe_blockbytes: " << substripe_blockbytes << ", pktybtes: " << pktbytes << endl;

    // 1. the index of the failed block
    int repairBlockIdx = stripemeta->getBlockIndex(blockName);
    cout << "Coor::repairBlockConv.repairBlockIdx: " << repairBlockIdx << endl;

    // 2. prepare blocklist and loclist
    int ecn = stripemeta->getECN();
    int eck = stripemeta->getECK();
    int ecw = stripemeta->getECW();

    vector<string> blocklist = stripemeta->getBlockList();
    vector<unsigned int> loclist = stripemeta->getLocList();

    //cout << "Coor::repairBlock.blocklist: " << endl;
    //for (int i=0; i<ecn; i++) {
    //    cout << "  " << blocklist[i] << ": " << RedisUtil::ip2Str(loclist[i]) << endl;
    //}

    // 3. figure out repairloc
    if (enforceip) {
        loclist[repairBlockIdx] = clientip;
    }
    unsigned int repairLoc = loclist[repairBlockIdx];

    // 4. prepare availidx
    vector<int> availIndex;
    vector<int> toRepairIndex;
    for (int i=0; i<ecn; i++) {
        for (int j=0; j<ecw; j++) {
            int pktidx = i*ecw+j;
            if (i == repairBlockIdx)
                toRepairIndex.push_back(pktidx);
            else
                availIndex.push_back(pktidx);
        }
    }
    //cout << "avail: ";
    //for (auto item: availIndex)
    //    cout << item << " ";
    //cout << endl;

    // 5. construct ECDAG
    ECBase* ec = stripemeta->createECClass();
    ecw = ec->_w;
    _stripeStore->lock();
    ECDAG* ecdag = ec->Decode(availIndex, toRepairIndex);
    _stripeStore->unlock();
    ecdag->Concact(toRepairIndex);

    // 6. divide ecdag into ecunits
    ecdag->genECUnits();

    // 7. get data structures from ecdag
    unordered_map<int, ECNode*> ecNodeMap = ecdag->getECNodeMap();
    vector<int> ecHeaders = ecdag->getECHeaders();
    vector<int> ecLeaves = ecdag->getECLeaves();
    unordered_map<int, ECUnit*> ecunits = ecdag->getUnitMap();
    vector<int> ecUnitList = ecdag->getUnitList();

    cout << "Total nodes: " << ecNodeMap.size() << endl;
    cout << "Header nodes: " << ecHeaders.size() << endl;
    cout << "Leaf nodes: " << ecLeaves.size() << endl;

    int intermediate_num = ecNodeMap.size() - ecHeaders.size() - ecLeaves.size();
    cout << "Intermediate nodes: " << intermediate_num << endl;

    vector<ECTask*> tasklist;
    ecdag->genConvECTasksWithOffset(tasklist, ecn, eck, ecw, substripe_blockbytes, pktbytes, stripename, blocklist, loclist, repairBlockIdx, substripe_offset);

    gettimeofday(&time2, NULL);

    // 8. send out tasks
    //cout << "ECTasks: " << endl;
    for (int i=0; i<tasklist.size(); i++) {

        //if (i > 4)
        //    break;

        ECTask* task = tasklist[i];
        //cout << "  " << tasklist[i]->dumpStr() << endl;
        task->sendTask(i);
    }

    // delete
    delete ec;
    delete ecdag;
    for (int i=0; i<tasklist.size(); i++) {
        delete tasklist[i];
    }
}
