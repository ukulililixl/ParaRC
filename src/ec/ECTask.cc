#include "ECTask.hh"

ECTask::ECTask() {
}

ECTask::~ECTask() {
}

void ECTask::buildReadDisk(int type,
               unsigned int loc,
               string blockname,
               int ecw,
               vector<int> indices,
               string stripename) {
  _type = type;
  _loc = loc;
  _blockName = blockname;
  _ecw = ecw;
  _indices = indices;
  _stripeName = stripename;
}

void ECTask::buildFetchCompute(int type,
               unsigned int loc,
               vector<int> prevIndices,
               vector<unsigned> prevLocs,
               vector<ComputeTask*> computelist,
               string stripename,
               vector<int> indices,
               int ecw) {
  _type = type;
  _loc = loc;
  _prevIndices = prevIndices;
  _prevLocs = prevLocs;
  _computeTaskList = computelist;
  _stripeName = stripename;
  _indices = indices;
  _ecw = ecw;
}

void ECTask::buildConcatenate(int type,
               unsigned int loc,
               vector<int> prevIndices,
               vector<unsigned int> prevLocs,
               string stripename,
               string blockname,
               int ecw) {
  _type = type;
  _loc = loc;
  _prevIndices = prevIndices;
  _prevLocs = prevLocs;
  _stripeName = stripename;
  _blockName = blockname;
  _ecw = ecw;
}

string ECTask::dumpStr() {
  string toret;
  toret += "Send to " + RedisUtil::ip2Str(_loc) + ": \n";
  if (_type == 0) {
    toret += "  read block: " + _blockName + ", \n";
    toret += "  packetIdx: ";
    for (auto pktidx: _indices) 
      toret += to_string(pktidx) + " ";
    toret += "\n";
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
    agcmd->buildType0(0, _loc, _blockName, _ecw, _indices, _stripeName);
  } else if (_type == 1) {
    agcmd->buildType1(1, _loc, _prevIndices, _prevLocs, _computeTaskList, _stripeName, _indices, _ecw);
  } else if (_type == 2) {
    agcmd->buildType2(2, _loc, _prevIndices, _prevLocs, _stripeName, _blockName, _ecw);
  }
  return agcmd;
}
