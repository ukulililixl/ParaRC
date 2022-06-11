#include "ECTask.hh"

ECTask::ECTask() {
}

ECTask::~ECTask() {
  if (_computeTaskList.size()) {
    for (auto t: _computeTaskList)
      delete t;
  }
}

void ECTask::buildReadDisk(int type,
        unsigned int loc,
        string blockname,
        int blkbytes,
        int pktbytes,
        int ecw,
        unordered_map<int, int> cid2ref,
        string stripename) {
    _type = type;
    _loc = loc;
    _blockName = blockname;
    _blkBytes = blkbytes;
    _pktBytes = pktbytes;
    _ecw = ecw;
    _cid2refs = cid2ref;
    _stripeName = stripename;
}

void ECTask::buildFetchCompute(int type,
               unsigned int loc,
               vector<int> prevIndices,
               vector<unsigned> prevLocs,
               vector<ComputeTask*> computelist,
               string stripename,
               //vector<int> indices,
               unordered_map<int, int> cid2refs,
               int ecw, int blkbytes, int pktbytes) {
  _type = type;
  _loc = loc;
  _prevIndices = prevIndices;
  _prevLocs = prevLocs;
  _computeTaskList = computelist;
  _stripeName = stripename;
  //_indices = indices;
  _cid2refs = cid2refs;
  _ecw = ecw;
  _blkBytes = blkbytes;
  _pktBytes = pktbytes;
}

void ECTask::buildConcatenate(int type,
               unsigned int loc,
               vector<int> prevIndices,
               vector<unsigned int> prevLocs,
               string stripename,
               string blockname,
               int ecw, int blkbytes, int pktbytes) {
  _type = type;
  _loc = loc;
  _prevIndices = prevIndices;
  _prevLocs = prevLocs;
  _stripeName = stripename;
  _blockName = blockname;
  _ecw = ecw;
  _blkBytes = blkbytes;
  _pktBytes = pktbytes;
}

int ECTask::getType() {
  return _type;
}

vector<ComputeTask*> ECTask::getComputeTaskList() {
  return _computeTaskList;
}

string ECTask::dumpStr() {
  string toret;
  toret += "Send to " + RedisUtil::ip2Str(_loc) + ": \n";
  if (_type == 0) {
    toret += "  read block: " + _blockName + ", \n";
    for (auto item: _cid2refs) {
        toret += "  cid: " + to_string(item.first) + ", refs: " + to_string(item.second) + ", \n";
    }
    toret += "  blkbytes: " + to_string(_blkBytes) + ", \n";
    toret += "  pktbytes: " + to_string(_pktBytes) + ", \n";
    toret += "  ecw: " + to_string(_ecw) + ", \n";
    toret += "  cache key prefix: " + _stripeName + "\n";
  } else if (_type == 1) {
    for (int i=0; i<_prevIndices.size(); i++) {
      toret += "  fetch: " + _stripeName + ":" + to_string(_prevIndices[i]) + " from " + RedisUtil::ip2Str(_prevLocs[i]) + "\n";
    }
    for (int i=0; i<_computeTaskList.size(); i++) {
      toret += "  compute: ";
      vector<int> srclist = _computeTaskList[i]->_srclist; 
      vector<int> dstlist = _computeTaskList[i]->_dstlist; 
      vector<vector<int>> coefs = _computeTaskList[i]->_coefs; 
      
      toret += "    srclist: ";
      for (auto item: srclist)
        toret += to_string(item) + " ";
      toret += "\n";
      
      for (int j=0; j<dstlist.size(); j++) {
        toret += "    dstidx: " + to_string(dstlist[j]) + "\n";
        vector<int> coef = coefs[j];
        toret += "      coef: ";
        for (int l=0; l<coef.size(); l++)
          toret += to_string(coef[l]) + " ";
        toret += "\n";
      }
      toret += "  cache key prefix: " + _stripeName + "\n";
    }
  } else if (_type == 2) {
    for (int i=0; i<_prevIndices.size(); i++) {
      toret += "  fetch: " + _stripeName + ":" + to_string(_prevIndices[i]) + " from " + RedisUtil::ip2Str(_prevLocs[i]) + "\n";
    }
    toret += "  concatenate to " + _blockName + "\n";
  }
  return toret;
}

AGCommand* ECTask::genAGCommand() {
  AGCommand* agcmd = new AGCommand();
  if (_type == 0) {
    // this is read disk command
    agcmd->buildType0(0, _loc, _blockName, _blkBytes, _pktBytes, _ecw, _cid2refs, _stripeName);
  } else if (_type == 1) {
    agcmd->buildType1(1, _loc, _prevIndices, _prevLocs, _computeTaskList, _stripeName, _cid2refs, _ecw, _blkBytes, _pktBytes);
  } else if (_type == 2) {
    agcmd->buildType2(2, _loc, _prevIndices, _prevLocs, _stripeName, _blockName, _ecw, _blkBytes, _pktBytes);
  }
  return agcmd;
}
