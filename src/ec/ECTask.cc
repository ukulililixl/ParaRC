#include "ECTask.hh"

ECTask::ECTask() {
}

ECTask::~ECTask() {
    if (_computeTaskList.size() > 0) {
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

void ECTask::buildReadDiskWithOffset(int type,
        unsigned int loc,
        string blockname,
        int blkbytes,
        int pktbytes,
        int ecw,
        unordered_map<int, int> cid2ref,
        string stripename, 
        int offset) {
    _type = type;
    _loc = loc;
    _blockName = blockname;
    _blkBytes = blkbytes;
    _pktBytes = pktbytes;
    _ecw = ecw;
    _cid2refs = cid2ref;
    _stripeName = stripename;
    _offset = offset;
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

void ECTask::buildFetchCompute2(int type,
        unsigned int loc,
        unordered_map<unsigned int, vector<int>> ip2cidlist,
        vector<ComputeTask*> computelist,
        string stripename,
        unordered_map<int, int> cid2refs,
        int ecw, int blkbytes, int pktbytes) {
    _type = type;
    _loc = loc;
    _ip2cidlist = ip2cidlist;
    _computeTaskList = computelist;
    _stripeName = stripename;
    _cid2refs = cid2refs;
    _ecw = ecw;
    _blkBytes = blkbytes;
    _pktBytes = pktbytes;
}

void ECTask::buildFetchCompute3(int type,
        unsigned int loc,
        unordered_map<unsigned int, vector<int>> ip2cidlist,
        vector<ComputeTask*> computelist,
        string stripename,
        unordered_map<int, int> cid2refs,
        int ecw, int blkbytes, int pktbytes, string blockname) {
    _type = type;
    _loc = loc;
    _ip2cidlist = ip2cidlist;
    _computeTaskList = computelist;
    _stripeName = stripename;
    _cid2refs = cid2refs;
    _ecw = ecw;
    _blkBytes = blkbytes;
    _pktBytes = pktbytes;
    _blockName = blockname;
}

void ECTask::buildConcatenate2(int type,                                                                                                                                                  
        unsigned int loc,                                                                                                                                                                              
        unordered_map<unsigned int, vector<int>> ip2cidlist,                                                                                                                                           
        string stripename, string blockname,
        int ecw, int blkbytes, int pktbytes) {
    _type = type;
    _loc = loc;
    _ip2cidlist = ip2cidlist;
    _stripeName = stripename;
    _blockName = blockname;
    _ecw = ecw;
    _blkBytes = blkbytes;
    _pktBytes = pktbytes;
}

void ECTask::buildReadCompute(int type, unsigned int loc,
        string blockname, int blkbytes, int pktbytes,
        vector<int> cidlist, int ecw, string stripename,
        vector<ComputeTask*> computelist,
        unordered_map<int, int> cid2refs) {
    _type = type;
    _loc = loc;
    _blockName = blockname;
    _blkBytes = blkbytes;
    _pktBytes = pktbytes;
    _prevIndices = cidlist;
    _ecw = ecw;
    _stripeName = stripename;
    _computeTaskList = computelist;
    _cid2refs = cid2refs;
}

void ECTask::buildReadComputeWithOffset(int type, unsigned int loc,
        string blockname, int blkbytes, int pktbytes,
        vector<int> cidlist, int ecw, string stripename,
        vector<ComputeTask*> computelist,
        unordered_map<int, int> cid2refs, int offset) {
    _type = type;
    _loc = loc;
    _blockName = blockname;
    _blkBytes = blkbytes;
    _pktBytes = pktbytes;
    _prevIndices = cidlist;
    _ecw = ecw;
    _stripeName = stripename;
    _computeTaskList = computelist;
    _cid2refs = cid2refs;
    _offset = offset;
}

void ECTask::sendTask(int taskid) {
    if (_type == 0) {
        // readDisk
        AGCommand* agcmd = new AGCommand();
        agcmd->buildType0(0, _loc, _blockName, _blkBytes, _pktBytes, _ecw, _cid2refs, _stripeName);
        agcmd->sendTo(_loc);
        delete agcmd;
    } else if (_type == 3) {
        // fetchAndCompute2
        AGCommand* agcmd = new AGCommand();
        agcmd->buildType3(3, _loc, _ip2cidlist, _computeTaskList, _stripeName, _cid2refs, _ecw, _blkBytes, _pktBytes, taskid);
        agcmd->sendTo(_loc);
        delete agcmd;

        // fetch commands
        for (auto item: _ip2cidlist) {
            unsigned int fetchip = item.first;
            vector<int> cidlist = item.second;
            FetchCommand* fcmd = new FetchCommand(_stripeName, fetchip, cidlist, taskid);
            fcmd->buildCommand();
            fcmd->sendTo(_loc);
            delete fcmd;
        }
        
        // compute commands
        for (int i=0; i<_computeTaskList.size(); i++) {
            ComputeTask* ct = _computeTaskList[i];
            vector<int> srclist = ct->_srclist;
            vector<int> dstlist = ct->_dstlist;
            vector<vector<int>> coefs = ct->_coefs;
            ComputeCommand* ccmd = new ComputeCommand(_stripeName, srclist, dstlist, coefs, taskid);
            ccmd->buildCommand();
            ccmd->sendTo(_loc);
            delete ccmd;
        }
        
        // cache commands
        CacheCommand* cacmd = new CacheCommand(_stripeName, _cid2refs, taskid);
        cacmd->buildCommand();
        cacmd->sendTo(_loc);
        delete cacmd;
    } else if (_type == 4) {
        // concatenate2
        AGCommand* agcmd = new AGCommand();
        agcmd->buildType4(4, _loc, _ip2cidlist, _stripeName, _blockName, _ecw, _blkBytes, _pktBytes, taskid);
        agcmd->sendTo(_loc);
        delete agcmd;

        // fetch commands
        for (auto item: _ip2cidlist) {
            unsigned int fetchip = item.first;
            vector<int> cidlist = item.second;
            FetchCommand* fcmd = new FetchCommand(_stripeName, fetchip, cidlist, taskid);
            fcmd->buildCommand();
            fcmd->sendTo(_loc);
            delete fcmd;
        }
    } else if (_type == 5) {
        AGCommand* agcmd = new AGCommand();
        agcmd->buildType5(5, _loc, _blockName, _blkBytes, _pktBytes, _prevIndices, 
                _ecw, _stripeName, _computeTaskList, _cid2refs, taskid);
        agcmd->sendTo(_loc);
        delete agcmd;

        // compute commands
        for (int i=0; i<_computeTaskList.size(); i++) {
            ComputeTask* ct = _computeTaskList[i];
            vector<int> srclist = ct->_srclist;
            vector<int> dstlist = ct->_dstlist;
            vector<vector<int>> coefs = ct->_coefs;
            ComputeCommand* ccmd = new ComputeCommand(_stripeName, srclist, dstlist, coefs, taskid);
            ccmd->buildCommand();
            ccmd->sendTo(_loc);
            delete ccmd;
        }
    } else if (_type == 6) {
        // readDisk
        AGCommand* agcmd = new AGCommand();
        agcmd->buildType6(6, _loc, _blockName, _blkBytes, _pktBytes, _ecw, _cid2refs, _stripeName, _offset);
        agcmd->sendTo(_loc);
        delete agcmd;
        //cout << "send type6 to " << RedisUtil::ip2Str(_loc) << endl;
    } else if (_type == 7) {

        AGCommand* agcmd = new AGCommand();
        agcmd->buildType7(7, _loc, _blockName, _blkBytes, _pktBytes, _prevIndices, 
                _ecw, _stripeName, _computeTaskList, _cid2refs, taskid, _offset);
        agcmd->sendTo(_loc);
        delete agcmd;

        // compute commands
        for (int i=0; i<_computeTaskList.size(); i++) {
            ComputeTask* ct = _computeTaskList[i];
            vector<int> srclist = ct->_srclist;
            vector<int> dstlist = ct->_dstlist;
            vector<vector<int>> coefs = ct->_coefs;
            ComputeCommand* ccmd = new ComputeCommand(_stripeName, srclist, dstlist, coefs, taskid);
            ccmd->buildCommand();
            ccmd->sendTo(_loc);
            delete ccmd;
        }
    } else if (_type == 8) {

        // fetchAndCompute2
        AGCommand* agcmd = new AGCommand();
        agcmd->buildType8(8, _loc, _ip2cidlist, _computeTaskList, _stripeName, _cid2refs, _ecw, _blkBytes, _pktBytes, taskid, _blockName);
        agcmd->sendTo(_loc);
        delete agcmd;

        // fetch commands
        for (auto item: _ip2cidlist) {
            unsigned int fetchip = item.first;
            vector<int> cidlist = item.second;
            FetchCommand* fcmd = new FetchCommand(_stripeName, fetchip, cidlist, taskid);
            fcmd->buildCommand();
            fcmd->sendTo(_loc);
            delete fcmd;
        }
        
        // compute commands
        for (int i=0; i<_computeTaskList.size(); i++) {
            ComputeTask* ct = _computeTaskList[i];
            vector<int> srclist = ct->_srclist;
            vector<int> dstlist = ct->_dstlist;
            vector<vector<int>> coefs = ct->_coefs;
            ComputeCommand* ccmd = new ComputeCommand(_stripeName, srclist, dstlist, coefs, taskid);
            ccmd->buildCommand();
            ccmd->sendTo(_loc);
            delete ccmd;
        }
        
        // cache commands
        CacheCommand* cacmd = new CacheCommand(_stripeName, _cid2refs, taskid);
        cacmd->buildCommand();
        cacmd->sendTo(_loc);
        delete cacmd;
    }
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
  } else if (_type == 3) {
    for (auto item: _ip2cidlist) {
        unsigned int ip = item.first;
        vector<int> cidlist = item.second;
        toret += "  fetch from " + RedisUtil::ip2Str(ip) + ": ";
        for (int i=0; i<cidlist.size(); i++)
            toret += to_string(cidlist[i]) + " ";
        toret += "\n";
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
  } else if (_type == 4) {
    for (auto item: _ip2cidlist) {
        unsigned int ip = item.first;
        vector<int> cidlist = item.second;
        toret += "  fetch from " + RedisUtil::ip2Str(ip) + ": ";
        for (int i=0; i<cidlist.size(); i++)
            toret += to_string(cidlist[i]) + " ";
        toret += "\n";
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
