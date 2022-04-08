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
    //case 3: resolveType3(); break;
    //case 5: resolveType5(); break;
    //case 7: resolveType7(); break;
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

void AGCommand::buildType0(int type,
                           unsigned int sendip,
                           string blockname, 
                           int ecw, 
                           vector<int> indices, 
                           string stripename) {

  // setup corresponding parameters
  _type = 0;
  _sendIp = sendip;
  _blockName = blockname;
  _ecw = ecw;
  _indices = indices;
  _stripeName = stripename;

  // 1. type
  writeInt(_type);
  // 2. blockname
  writeString(blockname);
  // 3. ecw
  writeInt(_ecw);
  // 4. indices
  // 4.1 the number of indices 
  writeInt(indices.size());
  // 4.2 indices in a loop
  for (int i=0; i<indices.size(); i++)
    writeInt(indices[i]);
  // 5. stripename
  writeString(stripename);
}

void AGCommand::resolveType0() {
  // 2. blockname
  _blockName = readString();
  // 3. ecw
  _ecw = readInt();
  // 4. indices
  // 4.1 the number of indices
  int num = readInt();
  // 4.2 indices
  for (int i=0; i<num; i++) {
    _indices.push_back(readInt());
  }
  // 5. stripename
  _stripeName = readString();
}

void AGCommand::buildType1(int type, unsigned int sendip, vector<int> prevIndices, vector<unsigned int> prevLocs,
                    vector<ComputeTask*> ctlist, string stripename, vector<int> cacheIndices, int ecw) {

  // setup corresponding parameters
  _type = 1;
  _sendIp = sendip;
  _prevIndices = prevIndices;
  _prevLocs = prevLocs;
  _ctlist = ctlist;
  _stripeName = stripename;
  _indices = cacheIndices;

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
  // 6. cacheIndices
  int ncache = cacheIndices.size();
  writeInt(ncache);
  for (int i=0; i<ncache; i++) {
    writeInt(cacheIndices[i]);
  }
  // 7. ecw
  writeInt(ecw);
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
  // 6. cacheIndices
  int ncache = readInt();
  for (int i=0; i<ncache; i++) {
    _indices.push_back(readInt());
  }
  // 7. ecw
  _ecw = readInt();
}

void AGCommand::buildType2(int type, unsigned int sendip, vector<int> prevIndices, vector<unsigned int> prevLocs,
                    string stripename, string blockname, int ecw) {

  // setup corresponding parameters
  _type = 2;
  _sendIp = sendip;
  _prevIndices = prevIndices;
  _prevLocs = prevLocs;
  _stripeName = stripename;
  _blockName = blockname;
  _ecw = ecw;

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
}

string AGCommand::dumpStr() {
  string toret;
  if (_type == 0) {
    toret += "readDisk: block = " + _blockName + ", ecw: " + to_string(_ecw) + ", indices: ";
    for (int i=0; i<_indices.size(); i++)
      toret += to_string(_indices[i]) + " "; 
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
        toret += to_string(item);
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
  } else if (_type == 2) {
    toret += "concatenate: \n";
    for (int i=0; i<_prevIndices.size(); i++)
      toret += "  fetch " + to_string(_prevIndices[i]) + " from " + RedisUtil::ip2Str(_prevLocs[i]) + "\n";
    toret += "  concatenate into " + _blockName + "\n";
  }
  return toret;
}
