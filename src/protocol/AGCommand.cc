#include "AGCommand.hh"

AGCommand::AGCommand() {
  _agCmd = (char*)calloc(MAX_COMMAND_LEN, sizeof(char));
  _cmLen = 0;
  _rKey = "ag_request";
}

AGCommand::~AGCommand() {
  if (_agCmd) {
    free(_agCmd);
    _agCmd = 0;
  }
  _cmLen = 0;
  for (auto item: _ctlist)
      delete item;
}

AGCommand::AGCommand(char* reqStr) {
  _agCmd = reqStr;
  _cmLen = 0; 

  // parse type
  _type = readInt();

  switch(_type) {
    case 0: resolveType0(); break;
    case 1: resolveType1(); break;
    case 2: resolveType2(); break;
    case 3: resolveType3(); break;
    case 4: resolveType4(); break;
    case 5: resolveType5(); break;
    case 6: resolveType6(); break;
    case 7: resolveType7(); break;
    //case 10: resolveType10(); break;
    //case 11: resolveType11(); break;
    default: break;
  }
  _agCmd = nullptr;
  _cmLen = 0;
}

void AGCommand::writeInt(int value) {
  int tmpv = htonl(value);
  memcpy(_agCmd + _cmLen, (char*)&tmpv, 4); _cmLen += 4;
}

void AGCommand::writeString(string s) {
  int slen = s.length();
  int tmpslen = htonl(slen);
  // string length
  memcpy(_agCmd + _cmLen, (char*)&tmpslen, 4); _cmLen += 4;
  // string
  memcpy(_agCmd + _cmLen, s.c_str(), slen); _cmLen += slen;
}

int AGCommand::readInt() {
  int tmpint;
  memcpy((char*)&tmpint, _agCmd + _cmLen, 4); _cmLen += 4;
  return ntohl(tmpint);
}

string AGCommand::readString() {
  string toret;
  int slen = readInt();
  char* sname = (char*)calloc(sizeof(char), slen+1);
  memcpy(sname, _agCmd + _cmLen, slen); _cmLen += slen;
  toret = string(sname);
  free(sname);
  return toret;
}

char* AGCommand::getCmd() {
  return _agCmd;
}

int AGCommand::getCmdLen() {
  return _cmLen;
}

int AGCommand::getType() {
  return _type;
}

unsigned int AGCommand::getSendIp() {
  return _sendIp;
}

string AGCommand::getBlockName() {
  return _blockName;
}

int AGCommand::getECW() {
  return _ecw;
}

vector<int> AGCommand::getIndices() {
  return _indices;
}

string AGCommand::getStripeName() {
  return _stripeName;
}

vector<int> AGCommand::getPrevIndices() {
  return _prevIndices;
}

vector<unsigned int> AGCommand::getPrevLocs() {
  return _prevLocs;
}

vector<ComputeTask*> AGCommand::getCTList() {
  return _ctlist;
}

unordered_map<int, int> AGCommand::getCid2Refs() {
    return _cid2refs;
}

int AGCommand::getBlkBytes() {
    return _blkbytes;
}

int AGCommand::getPktBytes() {
    return _pktbytes;
}

int AGCommand::getTaskid() {
    return _taskid;
}

int AGCommand::getNFetchStream() {
    return _nFetchStream;
}

int AGCommand::getNCompute() {
    return _nCompute;
}

int AGCommand::getNOutCids() {
    return _nOutCids;
}

int AGCommand::getOffset() {
    return _offset;
}

void AGCommand::sendTo(unsigned int ip) {
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* sendReply = (redisReply*)redisCommand(sendCtx, "RPUSH ag_request %b", _agCmd, _cmLen);
    freeReplyObject(sendReply);
    redisFree(sendCtx);
}

void AGCommand::buildType0(int type,
                           unsigned int sendip,
                           string blockname, 
                           int blkbytes,
                           int pktbytes,
                           int ecw, 
                           unordered_map<int, int> cid2ref,
                           string stripename) {

  // setup corresponding parameters
  _type = 0;
  _sendIp = sendip;
  _blockName = blockname;
  _blkbytes = blkbytes;
  _pktbytes = pktbytes;
  _ecw = ecw;
  _cid2refs = cid2ref;
  _stripeName = stripename;

  // 1. type
  writeInt(_type);
  // 2. blockname
  writeString(blockname);
  // 3. blkbytes
  writeInt(_blkbytes);
  // 4. pktbytes
  writeInt(_pktbytes);
  // 5. ecw
  writeInt(_ecw);
  // 6. cid2refs
  // 6.1 the number of indices 
  writeInt(_cid2refs.size());
  // 6.2 indices in a loop
  for (auto item: cid2ref) {
      int cid = item.first;
      int ref = item.second;
      writeInt(cid);
      writeInt(ref);
  }
  // 7. stripename
  writeString(stripename);
}

void AGCommand::resolveType0() {
  // 2. blockname
  _blockName = readString();
  // 3. blkbytes
  _blkbytes = readInt();
  // 4. pktbytes
  _pktbytes = readInt();
  // 5. ecw
  _ecw = readInt();
  // 6. cid2refs
  int num = readInt();
  for (int i=0; i<num; i++) {
      int cid = readInt();
      int ref = readInt();
      _cid2refs.insert(make_pair(cid, ref));
  }
  // 7. stripename
  _stripeName = readString();
}

void AGCommand::buildType1(int type, 
        unsigned int sendip, 
        vector<int> prevIndices, 
        vector<unsigned int> prevLocs,
        vector<ComputeTask*> ctlist, 
        string stripename, 
        //vector<int> cacheIndices, 
        unordered_map<int, int> cid2refs,
        int ecw,
        int blkbytes,
        int pktbytes) {

  // setup corresponding parameters
  _type = 1;
  _sendIp = sendip;
  _prevIndices = prevIndices;
  _prevLocs = prevLocs;
  _ctlist = ctlist;
  _stripeName = stripename;
  //_indices = cacheIndices;
  _cid2refs = cid2refs;
  _ecw = ecw;
  _blkbytes = blkbytes;
  _pktbytes = pktbytes;

  // 1. type
  writeInt(type);
  // 2. prevIndices
  int nprev = prevIndices.size();
  writeInt(nprev);
  for (int i=0; i<nprev; i++) {
    writeInt(prevIndices[i]);
  }
  // 3. prevLocs
  for (int i=0; i<nprev; i++) {
    writeInt(prevLocs[i]);
  }
  // 4. compute task
  int nctask = ctlist.size();
  writeInt(nctask);
  for (int i=0; i<nctask; i++) {
    ComputeTask* ct = ctlist[i];
    vector<int> srclist = ct->_srclist;
    vector<int> dstlist = ct->_dstlist;
    vector<vector<int>> coefs = ct->_coefs;
    // 4.1 srclist
    int nsrc = srclist.size();
    writeInt(nsrc);
    for (int j=0; j<nsrc; j++) {
      writeInt(srclist[j]);
    }
    // 4.1 dstlist
    int ndst = dstlist.size();
    writeInt(ndst);
    for (int j=0; j<ndst; j++) {
      writeInt(dstlist[j]);
    }
    // 4.3 coefs
    for (int di=0; di<ndst; di++) {
      for (int si=0; si<nsrc; si++) {
        writeInt(coefs[di][si]);
      }
    }
  }
  // 5. stripename
  writeString(stripename);
  // 6. cid2refs
  int ncache = cid2refs.size();
  writeInt(ncache);
  for (auto item: cid2refs) {
      int cid = item.first;
      int r = item.second;
      writeInt(cid);
      writeInt(r);
  }
  // 7. ecw
  writeInt(ecw);
  // 8. blkbytes
  writeInt(_blkbytes);
  // 9. pktbytes
  writeInt(_pktbytes);
}

void AGCommand::resolveType1() {

  // 2. prevIndices
  int nprev = readInt();
  for (int i=0; i<nprev; i++) {
    _prevIndices.push_back(readInt());
  }
  // 3. prevLocs
  for (int i=0; i<nprev; i++) {
    _prevLocs.push_back(readInt());
  }
  // 4. compute task
  int nctask = readInt();
  for (int i=0; i<nctask; i++) {
    vector<int> srclist;
    vector<int> dstlist;
    vector<vector<int>> coefs;
    // 4.1 srclist
    int nsrc = readInt();
    for (int j=0; j<nsrc; j++) {
      srclist.push_back(readInt());
    }
    // 4.1 dstlist
    int ndst = readInt();
    for (int j=0; j<ndst; j++) {
      dstlist.push_back(readInt());
    }
    // 4.3 coefs
    for (int di=0; di<ndst; di++) {
      vector<int> tmplist;
      for (int si=0; si<nsrc; si++) {
        tmplist.push_back(readInt());
      }
      coefs.push_back(tmplist);
    }
    ComputeTask* ct = new ComputeTask(srclist, dstlist, coefs);
    _ctlist.push_back(ct);
  }
  // 5. stripename
  _stripeName = readString(); 
  // 6. cid2refs
  int ncache = readInt();
  for (int i=0; i<ncache; i++) {
    int cid = readInt();
    int r = readInt();
    _cid2refs.insert(make_pair(cid, r));
  }
  // 7. ecw
  _ecw = readInt();
  // 8. blkbytes
  _blkbytes = readInt();
  // 9. pktbytes
  _pktbytes = readInt();
}

void AGCommand::buildType2(int type, 
        unsigned int sendip, 
        vector<int> prevIndices, 
        vector<unsigned int> prevLocs,
        string stripename, 
        string blockname, 
        int ecw,
        int blkbytes,
        int pktbytes) {

  // setup corresponding parameters
  _type = 2;
  _sendIp = sendip;
  _prevIndices = prevIndices;
  _prevLocs = prevLocs;
  _stripeName = stripename;
  _blockName = blockname;
  _ecw = ecw;
  _blkbytes = blkbytes;
  _pktbytes = pktbytes;

  // 1. type
  writeInt(type);
  // 2. prevIndices
  int nprev = prevIndices.size();
  writeInt(nprev);
  for (int i=0; i<nprev; i++) {
    writeInt(prevIndices[i]);
  }
  // 3. prevLocs
  for (int i=0; i<nprev; i++) {
    writeInt(prevLocs[i]);
  }
  // 4. stripename
  writeString(stripename);
  // 5. blockname
  writeString(blockname);
  // 6. ecw
  writeInt(ecw);
  // 7. blkbytes
  writeInt(blkbytes);
  // 8. pktbytes
  writeInt(pktbytes);
}

void AGCommand::resolveType2() {
  // 2. prevIndices
  int nprev = readInt();
  for (int i=0; i<nprev; i++)
    _prevIndices.push_back(readInt());
  // 3. prevLocs
  for (int i=0; i<nprev; i++)
    _prevLocs.push_back(readInt());
  // 4. stripename
  _stripeName = readString();
  // 5. blockname
  _blockName = readString();
  // 6. ecw
  _ecw = readInt();
  // 7. blkbytes
  _blkbytes = readInt();
  // 8. pktbytes
  _pktbytes = readInt();
}

void AGCommand::buildType3(int type,
        unsigned int sendip,
        unordered_map<unsigned int, vector<int>> ip2cidlist,
        vector<ComputeTask*> ctlist,
        string stripename,
        unordered_map<int, int> cid2refs,
        int ecw,
        int blkbytes,
        int pktbytes,
        int taskid) {

    // set up parameters
    _type = type;
    _sendIp = sendip;
    _ip2cidlist = ip2cidlist;
    _ctlist;
    _stripeName = stripename;
    _cid2refs = cid2refs;
    _ecw = ecw;
    _blkbytes = blkbytes;
    _pktbytes = pktbytes;
    _taskid = taskid;

    // 1. type
    writeInt(type);
    // 2. number of fetching streams
    writeInt(ip2cidlist.size());
    // 3. number of compute tasks
    writeInt(ctlist.size());
    // 4. stripename
    writeString(stripename);
    // 5. number of output cids
    writeInt(cid2refs.size());
    // 6. ecw
    writeInt(ecw);
    // 7. blkbytes
    writeInt(blkbytes);
    // 8. pktbytes
    writeInt(pktbytes);
    // 9. taskid
    writeInt(taskid);
}

void AGCommand::resolveType3() {
    // 2. number of fetching streams
    _nFetchStream = readInt();
    // 3. number of compute tasks
    _nCompute = readInt();
    // 4. stripename 
    _stripeName = readString();
    // 5. number of output cids
    _nOutCids = readInt();
    // 6. ecw
    _ecw = readInt();
    // 7. blkbytes
    _blkbytes = readInt();
    // 8. pktbytes
    _pktbytes = readInt();
    // 9. taskid
    _taskid = readInt();
}

void AGCommand::buildType4(int type,
        unsigned int sendip,
        unordered_map<unsigned int, vector<int>> ip2cidlist,
        string stripename,
        string blockname,
        int ecw,
        int blkbytes,
        int pktbytes,
        int taskid) {

    // set up parameters
    _type = type;
    _sendIp = sendip;
    _ip2cidlist = ip2cidlist;
    _stripeName = stripename;
    _blockName = blockname;
    _ecw = ecw;
    _blkbytes = blkbytes;
    _pktbytes = pktbytes;
    _taskid = taskid;

    // 1. type
    writeInt(type);
    // 2. number of fetching streams
    writeInt(ip2cidlist.size());
    // 3. stripename
    writeString(stripename);
    // 4. blockname
    writeString(blockname);
    // 5. ecw
    writeInt(ecw);
    // 6. blkbytes
    writeInt(blkbytes);
    // 7. pktbytes
    writeInt(pktbytes);
    // 8. taskid
    writeInt(taskid);
}

void AGCommand::resolveType4() {
    // 2. number of fetching streams
    _nFetchStream = readInt();
    // 3. stripename 
    _stripeName = readString();
    // 4. blockname
    _blockName = readString();
    // 5. ecw
    _ecw = readInt();
    // 6. blkbytes
    _blkbytes = readInt();
    // 7. pktbytes
    _pktbytes = readInt();
    // 8. taskid
    _taskid = readInt();
}

void AGCommand::buildType5(int type, unsigned int sendip, string blockname, int blkbytes, 
        int pktbytes, vector<int> cidlist, int ecw, string stripename, vector<ComputeTask*> ctlist,
        unordered_map<int, int> cid2refs, int taskid) {

    // set up parameters
    _type = type;
    _sendIp = sendip;
    _blockName = blockname;
    _blkbytes = blkbytes;
    _pktbytes = pktbytes;
    _indices = cidlist;
    _ecw = ecw;
    _stripeName = stripename;
    _ctlist;
    _cid2refs = cid2refs;
    _taskid = taskid;

    // 1. type
    writeInt(type);
    // 2. blockname
    writeString(blockname);
    // 3. blkbytes
    writeInt(blkbytes);
    // 4. pktbytes
    writeInt(pktbytes);
    // 5. cidlist
    writeInt(cidlist.size());
    for (int i=0; i<cidlist.size(); i++) {
        writeInt(cidlist[i]);
    }
    // 6. ecw
    writeInt(ecw);
    // 7. stripename
    writeString(stripename);
    // 8. ctnum
    writeInt(ctlist.size());
    // 9. cid2refs
    writeInt(cid2refs.size());
    for (auto item: cid2refs) {
        writeInt(item.first);
        writeInt(item.second);
    }
    // 10. taskid
    writeInt(taskid);
}

void AGCommand::resolveType5() {
    // 2. blockname
    _blockName = readString();
    // 3. blkbytes
    _blkbytes = readInt();
    // 4. pktbytes
    _pktbytes = readInt();
    // 5. cidlist
    int num = readInt();
    for (int i=0; i<num; i++) {
        _indices.push_back(readInt());
    }
    // 6. ecw
    _ecw = readInt();
    // 7. stripename
    _stripeName = readString();
    // 8. ctnum
    _nCompute = readInt();
    // 9. cid2refs
    num = readInt();
    for (int i=0; i<num; i++) {
        int k = readInt();
        int v = readInt();
        _cid2refs.insert(make_pair(k, v));
    }
    // 10. taskid
    _taskid = readInt();
}

void AGCommand::buildType6(int type,
                           unsigned int sendip,
                           string blockname, 
                           int blkbytes,
                           int pktbytes,
                           int ecw, 
                           unordered_map<int, int> cid2ref,
                           string stripename,
                           int offset) {

  // setup corresponding parameters
  _type = type;
  _sendIp = sendip;
  _blockName = blockname;
  _blkbytes = blkbytes;
  _pktbytes = pktbytes;
  _ecw = ecw;
  _cid2refs = cid2ref;
  _stripeName = stripename;
  _offset = offset;

  // 1. type
  writeInt(_type);
  // 2. blockname
  writeString(blockname);
  // 3. blkbytes
  writeInt(_blkbytes);
  // 4. pktbytes
  writeInt(_pktbytes);
  // 5. ecw
  writeInt(_ecw);
  // 6. cid2refs
  // 6.1 the number of indices 
  writeInt(_cid2refs.size());
  // 6.2 indices in a loop
  for (auto item: cid2ref) {
      int cid = item.first;
      int ref = item.second;
      writeInt(cid);
      writeInt(ref);
  }
  // 7. stripename
  writeString(stripename);
  // 8. offset
  writeInt(_offset);
}

void AGCommand::resolveType6() {
  // 2. blockname
  _blockName = readString();
  // 3. blkbytes
  _blkbytes = readInt();
  // 4. pktbytes
  _pktbytes = readInt();
  // 5. ecw
  _ecw = readInt();
  // 6. cid2refs
  int num = readInt();
  for (int i=0; i<num; i++) {
      int cid = readInt();
      int ref = readInt();
      _cid2refs.insert(make_pair(cid, ref));
  }
  // 7. stripename
  _stripeName = readString();
  // 8. offset
  _offset = readInt();
}

void AGCommand::buildType7(int type, unsigned int sendip, string blockname, int blkbytes, 
        int pktbytes, vector<int> cidlist, int ecw, string stripename, vector<ComputeTask*> ctlist,
        unordered_map<int, int> cid2refs, int taskid, int offset) {

    // set up parameters
    _type = type;
    _sendIp = sendip;
    _blockName = blockname;
    _blkbytes = blkbytes;
    _pktbytes = pktbytes;
    _indices = cidlist;
    _ecw = ecw;
    _stripeName = stripename;
    _ctlist;
    _cid2refs = cid2refs;
    _taskid = taskid;
    _offset = offset;

    // 1. type
    writeInt(type);
    // 2. blockname
    writeString(blockname);
    // 3. blkbytes
    writeInt(blkbytes);
    // 4. pktbytes
    writeInt(pktbytes);
    // 5. cidlist
    writeInt(cidlist.size());
    for (int i=0; i<cidlist.size(); i++) {
        writeInt(cidlist[i]);
    }
    // 6. ecw
    writeInt(ecw);
    // 7. stripename
    writeString(stripename);
    // 8. ctnum
    writeInt(ctlist.size());
    // 9. cid2refs
    writeInt(cid2refs.size());
    for (auto item: cid2refs) {
        writeInt(item.first);
        writeInt(item.second);
    }
    // 10. taskid
    writeInt(taskid);
    // 11. offset
    writeInt(offset);
}

void AGCommand::resolveType7() {
    // 2. blockname
    _blockName = readString();
    // 3. blkbytes
    _blkbytes = readInt();
    // 4. pktbytes
    _pktbytes = readInt();
    // 5. cidlist
    int num = readInt();
    for (int i=0; i<num; i++) {
        _indices.push_back(readInt());
    }
    // 6. ecw
    _ecw = readInt();
    // 7. stripename
    _stripeName = readString();
    // 8. ctnum
    _nCompute = readInt();
    // 9. cid2refs
    num = readInt();
    for (int i=0; i<num; i++) {
        int k = readInt();
        int v = readInt();
        _cid2refs.insert(make_pair(k, v));
    }
    // 10. taskid
    _taskid = readInt();
    // 11. offset
    _offset = readInt();
}

string AGCommand::dumpStr() {
  string toret;
  toret += "send to " + RedisUtil::ip2Str(_sendIp) + "; ";
  if (_type == 0) {
    toret += "readDisk: block = " + _blockName + ", ecw: " + to_string(_ecw) + ", indices: ";
    for (auto item: _cid2refs) {
        int cid = item.first;
        int ref = item.second;
        toret += to_string(cid) + "(" + to_string(ref) + ")" + " ";
    }
    toret += ", stripename: " + _stripeName + "\n";
  } else if (_type == 1) {
    toret += "fetchAndCompute: \n";
    for (int i=0; i<_prevIndices.size(); i++) {
      toret += "  fetch " + to_string(_prevIndices[i]) + " from " + RedisUtil::ip2Str(_prevLocs[i]) + "\n";
    }
    for (int i=0; i<_ctlist.size(); i++) {
      ComputeTask* ct = _ctlist[i];
      toret += "  compute: \n";
      vector<int> srclist = ct->_srclist;
      vector<int> dstlist = ct->_dstlist;
      vector<vector<int>> coefs = ct->_coefs;
      toret += "    srclist: ";
      for (auto item: srclist)
        toret += to_string(item) + " ";
      toret += "\n";
      for (int j=0; j<dstlist.size(); j++) {
        toret +="    dst " + to_string(dstlist[j]) + ": ";
        for (auto c: coefs[j])
          toret += to_string(c) + " ";
        toret += "\n";
      }
    }
    toret += "  keybase: " + _stripeName;
    for (auto item: _indices) {
      toret += "  cache: " + to_string(item) + " ";
    }
    toret += "\n";
  } else if (_type == 2) {
    toret += "concatenate: \n";
    for (int i=0; i<_prevIndices.size(); i++)
      toret += "  fetch " + to_string(_prevIndices[i]) + " from " + RedisUtil::ip2Str(_prevLocs[i]) + "\n";
    toret += "  concatenate into " + _blockName + "\n";
  }
  return toret;
}
