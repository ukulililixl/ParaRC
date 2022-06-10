#include "ECDAG.hh"

ECDAG::ECDAG() {
}

ECDAG::~ECDAG() {
  for (auto it: _ecNodeMap) {
    if (it.second) {
      delete it.second;
      it.second = nullptr;
    }
  }
  _ecNodeMap.clear();
  
  for (auto it: _sgMap) {
    if (it.second) {
      delete it.second;
      it.second = nullptr;
    }
  }
}

//int ECDAG::findCluster(vector<int> childs) {
//  for (int i=0; i<_clusterMap.size(); i++) {
//    if (_clusterMap[i]->childsInCluster(childs)) return i;
//  }
//  return -1;
//}

void ECDAG::Join(int pidx, vector<int> cidx, vector<int> coefs) {
  // // debug start
  // string msg = "ECDAG::Join(" + to_string(pidx) + ",";
  // for (int i=0; i<cidx.size(); i++) msg += " "+to_string(cidx[i]);
  // msg += ",";
  // for (int i=0; i<coefs.size(); i++) msg += " "+to_string(coefs[i]);
  // msg += ")";
  // if (ECDAG_DEBUG_ENABLE) cout << msg << endl;
  // // debug end

  // 0. deal with childs
  vector<ECNode*> targetChilds;
  for (int i=0; i<cidx.size(); i++) { 
    int curId = cidx[i];
    // 0.0 check whether child exists in our ecNodeMap
    unordered_map<int, ECNode*>::const_iterator findNode = _ecNodeMap.find(curId);
    ECNode* curNode;
    if (findNode == _ecNodeMap.end()) {
      // child does not exists, we need to create a new one
      curNode = new ECNode(curId);
      // a new child is set to leaf on creation
      curNode->setType("leaf");
      // insert into ecNodeMap
      _ecNodeMap.insert(make_pair(curId, curNode));
      _ecLeaves.push_back(curId);
    } else {
      // child exists
      curNode = _ecNodeMap[curId];
      // NOTE: if this node is marked with root before, it should be marked as intermediate now
      vector<int>::iterator findpos = find(_ecHeaders.begin(), _ecHeaders.end(), curId);
      if (findpos != _ecHeaders.end()) {
        // delete from headers
        _ecHeaders.erase(findpos);
        curNode->setType("intermediate");
      }
    }
    // 0.1 add curNode into targetChilds
    targetChilds.push_back(curNode);
//    // 0.3 increase refNo for curNode
//    curNode->incRefNumFor(curId);
  }

  // 1. deal with parent
  ECNode* rNode;
  assert(_ecNodeMap.find(pidx) == _ecNodeMap.end());
  // pidx does not exists, create new one and add to headers
  rNode = new ECNode(pidx);
  // parent node is set to root on creation
  rNode->setType("root");
  _ecNodeMap.insert(make_pair(pidx, rNode));
  _ecHeaders.push_back(pidx);

  // set child nodes for the root node, as well as the coefs
  rNode->setChilds(targetChilds);
  rNode->setCoefs(coefs);
//  rNode->addCoefs(pidx, coefs);

  // set parent node for each child node
  for (auto cnode: targetChilds) {
    cnode->addParentNode(rNode);
  }

//  // 2. deal with cluster
//  vector<int> childs(cidx);
//  sort(childs.begin(), childs.end());
//  int clusterIdx = findCluster(childs);
//  Cluster* curCluster;
//  if (clusterIdx == -1) {
//    // cluster does not exists, create new cluster
//    curCluster = new Cluster(childs, pidx);
//    _clusterMap.push_back(curCluster);
//  } else {
//    curCluster = _clusterMap[clusterIdx];
//    curCluster->addParent(pidx);
//  }
}

void ECDAG::Concact(vector<int> cidx) {
  vector<int> coefs;
  for (int i=0; i<cidx.size(); i++) {
    coefs.push_back(-1);
  }

  Join(REQUESTOR, cidx, coefs);
}

void ECDAG::genECUnits() {
  // 0. generate inrefs and outrefs for each node
  unordered_map<int, int> inref;
  unordered_map<int, int> outref;
  for (auto item: _ecNodeMap) {
    int nodeId = item.first;
    ECNode* node = item.second;
    vector<ECNode*> childlist = node->getChildNodes();
    // increase outref for each child by 1
    for (auto cnode: childlist) {
      int cidx = cnode->getNodeId();
      if (outref.find(cidx) == outref.end())
        outref.insert(make_pair(cidx, 1));
      else
        outref[cidx]++;
    }
    // increate inref for current node by childlist.size();
    inref.insert(make_pair(nodeId, childlist.size()));
  }

//  cout << "ECDAG::genECUnits.inref:" << endl;
//  for (auto item: inref) {
//    cout <<   item.first << ", " << item.second << endl;
//  }
//  cout << "ECDAG::genECUnits.outref:" << endl;
//  for (auto item: outref) {
//    cout <<   item.first << ", " << item.second << endl;
//  }

  // 1. Each time we search for leaf nodes
  BlockingQueue<int> leaves;
  for (auto item: inref) {
    if (item.second == 0)
      leaves.push(item.first);
  }

  int debugnum=0;
  while (leaves.getSize() > 0) {
    int nodeId = leaves.pop();
    //cout << "Check node "    << nodeId << endl;

    // if the outref of this node is zero, skip
    if (outref[nodeId] == 0) {
      //cout << "  outref[" << nodeId << "] = 0, skip" << endl;
      continue;
    }
 
    // verify parent node for curnode
    ECNode* curNode = _ecNodeMap[nodeId];
    vector<ECNode*> parentNodes = curNode->getParentNodes();
    if (parentNodes.size() == 0) {
      //cout << "Reach the root, exit!"  << endl;
      break;
    }

    for (auto pitem: parentNodes) {
      int pnodeId = pitem->getNodeId();     
      ECNode* pnode = pitem;
      //cout << "  pnodeId: " << pnodeId << endl;
      // figure out all the childnode for the current node
      vector<ECNode*> tmpChildList = pnode->getChildNodes();
      vector<int> tmpChildIndices = pnode->getChildIndices();
      vector<int> tmpcoefs = pnode->getCoefs();

      bool allleaves = true;
      bool finished = false;
      for (int tmpi=0; tmpi<tmpChildIndices.size(); tmpi++) {
        int tmpcnodeId = tmpChildIndices[tmpi];
        int tmpinref = inref[tmpcnodeId];
        int tmpoutref = outref[tmpcnodeId];
        //cout << "    cnodeId: " << tmpcnodeId << ", inref: " << tmpinref << ", outref: " << tmpoutref << endl;
        if (tmpinref != 0)
          allleaves = false;
        if (tmpoutref == 0)
          finished = true;
      }

      if (!finished & allleaves) {
        // generate ECUnit
        ECUnit* ecunit = new ECUnit(_unitId++, tmpChildIndices, pnodeId, tmpcoefs);
        _ecUnitMap.insert(make_pair(ecunit->getUnitId(), ecunit));
        _ecUnitList.push_back(ecunit->getUnitId());

        // decrease child outref by 1
        for (int tmpi=0; tmpi<tmpChildIndices.size(); tmpi++) {
          int tmpcnodeId = tmpChildIndices[tmpi];
          outref[tmpcnodeId]--;
        }
        // decrease parent inref by childsize
        inref[pnodeId] -= tmpChildIndices.size();
        if (inref[pnodeId] == 0)
          leaves.push(pnodeId);
      }
    }

    // after checking all the parent of this node
    if (outref[nodeId] != 0)
      leaves.push(nodeId);

    //debugnum++;
    //if (debugnum > 2)
    //  break;
  }

  // debug
  for (int i=0; i<_ecUnitList.size(); i++)  {
    cout << _ecUnitMap[_ecUnitList[i]]->dump() << endl;
  }
}

void ECDAG::clearECCluster() {
  for (auto item: _ecClusterMap)
    delete item.second;
  _clusterId = CSTART;
  _ecClusterMap.clear();
  _ecClusterList.clear();
}

void ECDAG::genECCluster(unordered_map<int, int> coloring) {
  // 0. generate inrefs and outrefs for each node
  unordered_map<int, int> inref;
  unordered_map<int, int> outref;
  for (auto item: _ecNodeMap) {
    int nodeId = item.first;
    ECNode* node = item.second;
    vector<ECNode*> childlist = node->getChildNodes();
    // increase outref for each child by 1
    for (auto cnode: childlist) {
      int cidx = cnode->getNodeId();
      if (outref.find(cidx) == outref.end())
        outref.insert(make_pair(cidx, 1));
      else
        outref[cidx]++;
    }
    // increase inref for current node by childlist.size();
    inref.insert(make_pair(nodeId, childlist.size()));
  }

  unordered_map<int, int> leaves;
  for (auto item: inref) {
    if (item.second == 0)
      leaves.insert(make_pair(item.first, 1));
  }

  int starti = 0;
  while (leaves.size() > 0) {
    if (leaves.size() == 1) {
      bool root = false;
      for (auto item: leaves) {
        if (item.first == REQUESTOR)
          root = true;
      }
      if (root) {
        leaves.clear();
        break;
      }
    }

    //cout << "current leaves: ";
    //for (auto item: leaves)
    //  cout << item.first << " ";
    //cout << endl;

    vector<int> curunitlist;
    //cout << "starti: " << starti << ", all: " << _ecUnitList.size() << endl;
    for ( ; starti < _ecUnitList.size(); starti++) {
      int curunitidx = _ecUnitList[starti];
      ECUnit* curecunit = _ecUnitMap[curunitidx];
      // check whether all the child in this unit are leaves
      vector<int> curchildlist = curecunit->getChilds();

      bool flag = true;
      for (auto curcid: curchildlist) {
        if (leaves.find(curcid) == leaves.end()) {
          flag = false;
          break;
        }
      }

      //cout << "  unitidx: " << curunitidx << ", flag: " << flag << endl;
      if (flag) {
        // put curunitidx inout curunitlist
        curunitlist.push_back(curunitidx);
      } else { 
        // stop now
        break;
      }
    }

    //cout << "current layer of ECUnit: ";
    //for (int ui=0; ui<curunitlist.size(); ui++) {
    //  cout << curunitlist[ui] << " ";
    //}
    //cout << endl;

    // collect coloring for the parent in each unit
    unordered_map<int, vector<int>> color2unitlist;
    for (int ui=0; ui<curunitlist.size(); ui++) {
      int unitidx = curunitlist[ui];
      //cout << "  unitidx = " << unitidx << endl;
      ECUnit* curunit = _ecUnitMap[unitidx];
      int pnodeidx = curunit->getParent();
      int pcolor = coloring[pnodeidx];
      
      if (color2unitlist.find(pcolor) == color2unitlist.end()) {
        vector<int> tmplist = {unitidx};
        color2unitlist.insert(make_pair(pcolor, tmplist));
      } else
        color2unitlist[pcolor].push_back(unitidx);

      // for each unit, decrease outref for child by 1, decrease inref for parent by childnum
      vector<int> curchilds = curunit->getChilds();
      for (auto tmpc: curchilds) {
        outref[tmpc]--;
        if (outref[tmpc] == 0) {
          leaves.erase(leaves.find(tmpc));
        }
      }
      inref[pnodeidx]-=curchilds.size();
      if (inref[pnodeidx] == 0)
        leaves.insert(make_pair(pnodeidx, 1));
    }

    // for each color, merge corresponding unit
    for (auto item: color2unitlist) {
      vector<int> curunitlist = item.second;
      ECCluster* eccluster = new ECCluster(_clusterId++, curunitlist);
      _ecClusterMap.insert(make_pair(eccluster->getClusterId(), eccluster));
      _ecClusterList.push_back(eccluster->getClusterId());
    }
  }

  // // debug the ecclusters
  // for (int tmpi=0; tmpi<_ecClusterList.size(); tmpi++) {
  //   int clusterid = _ecClusterList[tmpi];
  //   ECCluster* cluster = _ecClusterMap[clusterid];
  //   cout << cluster->dump() << endl;
  // }
}

void ECDAG::genStat(unordered_map<int, int> coloring, 
                    unordered_map<int, int>& inmap, 
                    unordered_map<int, int>& outmap) {
  for (int i=0; i<_ecClusterList.size(); i++) {
    int clusterIdx = _ecClusterList[i];
    ECCluster* cluster = _ecClusterMap[clusterIdx];
    vector<int> unitlist = cluster->getUnitList();

    // collect all the unitchilds
    unordered_map<int, int> clusterChilds;
    int pcolor;
    for (int unitidx: unitlist) {
      ECUnit* unit = _ecUnitMap[unitidx];
      vector<int> tmpchilds = unit->getChilds();
      for (auto c: tmpchilds) {
        if (clusterChilds.find(c) == clusterChilds.end())
          clusterChilds.insert(make_pair(c, 1));
      }
      int pidx = unit->getParent();
      pcolor = coloring[pidx];
    }
    //cout << "clusterIdx: " << clusterIdx << ", pcolor: " << pcolor << endl;

    for (auto item: clusterChilds) {
      int ccolor = coloring[item.first];
      //cout << "  cid: " << item.first << ", ccolor: " << ccolor << endl;
      if (ccolor == -1) {
          // this is a virtual child, continue
          continue;
      }
      if (ccolor != pcolor) {
        if (outmap.find(ccolor) == outmap.end()) {
          outmap.insert(make_pair(ccolor, 1));
        } else {
          outmap[ccolor]++;
        }

        if (inmap.find(pcolor) == inmap.end()) {
          inmap.insert(make_pair(pcolor, 1));
        } else {
          inmap[pcolor]++;
        }
      }
      //cout << "    out[" << ccolor << "]: " <<outmap[ccolor] << ", in[" << pcolor << "]: " << inmap[pcolor] << endl;
    }
  }
}

void ECDAG::genSimpleGraph() {
  
  // 0. generate references for each node
  unordered_map<int, int> refMap;
  for (auto item: _ecNodeMap) {
    int nodeId = item.first;
    ECNode* node = item.second;
    refMap.insert(make_pair(nodeId, node->getNumChilds()));
  }
  // cout << "ECDAG::genSimpleGraph.refMap: " << endl;
  // for (auto item: refMap) {
  //   cout << "(" << item.first << ", " << item.second << ") ";
  // }
  // cout << endl;

  // 1. Each time we search for leaf nodes
  BlockingQueue<int> leaves;
  for (auto item: refMap) {
    if (item.second == 0)
      leaves.push(item.first);
  }    

  int debugnum = 0;
  while (leaves.getSize() > 0) {
    int nodeId = leaves.pop();
    // cout << "Check node " << nodeId << endl;

    // get all parent nodes for this node
    ECNode* curNode = _ecNodeMap[nodeId];
    vector<ECNode*> parentNodes = curNode->getParentNodes();
    if (parentNodes.size() == 0) {
      // cout << "  no parent" << endl;
      continue;
    }

    // we first mark it as process, if we cannot process it as this time, mark it as false and put it into the queue 
    bool process = false;
    // for each parent, check whether its childs are already in a cluster 
    for (auto pitem: parentNodes) {
      int mypid = pitem->getNodeId();
      // cout << "  mypid: " << mypid << endl;

      // if pid' child number is 0, then we have processed it
      if (refMap[mypid] == 0) {
        // cout << "    mypid " << mypid << " has been processed!" << endl;
        process = true;
        continue;
      }
      
      // get all the childIds
      vector<ECNode*> myChildNodes = pitem->getChildNodes();
      vector<int> myChildIds;
      for (auto citem: myChildNodes) 
        myChildIds.push_back(citem->getNodeId());
      // cout << "    myChildIds: ";
      // for (auto cid: myChildIds)
      //   cout << cid << " ";
      // cout << endl;

      // check whether each child is a leaf node
      bool proceed = true;
      for (auto cid: myChildIds) {
        if (refMap[cid] != 0) {
          // cout << "    child " << cid << " is not a leaf! cannot proceed with parentid " << mypid << endl;
          proceed = false;
          break;
        }
      }
      if (!proceed) {
        // continue to check the next parent
        continue;
      }

      // cout << "    all the childs are leaf nodes, we can proceed with parent " << mypid << endl;

      // get coefs for childs
      vector<int> myChildCoefs = pitem->getCoefs();
      unordered_map<int, int> myCoefMap;
      for (int i=0; i<myChildIds.size(); i++) {
        myCoefMap.insert(make_pair(myChildIds[i], myChildCoefs[i]));
      }
 
      // cout << "    myChildCoefs: ";
      // for (auto c: myChildCoefs)
      //   cout << c << " ";
      // cout << endl;

      // now we collect all the sgid that contains the cid of mypid
      vector<int> candidatesgids;
      for (auto cid: myChildIds) {
        if (_child2sgs.find(cid) != _child2sgs.end()) {
           for (int tmpsgid: _child2sgs[cid]) {
             if (find(candidatesgids.begin(), candidatesgids.end(), tmpsgid) == candidatesgids.end())
               candidatesgids.push_back(tmpsgid);
           }
        }
      }

      // cout << "    candidatesgids: ";
      // for (int tmpsgid: candidatesgids)
      //   cout << tmpsgid << " ";
      // cout << endl;

      // now for each sgid, we check whether the current myChildIds can fit into it.
      // if yes, we add the current parent into that sg
      // if not, we create new sg
      int action = 0; // 0: create; 1: add; -1: do nothing
      int addsgid = -1;
      for (int tmpsgid: candidatesgids) {
        SimpleGraph* tmpsg = _sgMap[tmpsgid];
        if (tmpsg->parentIn(mypid)) {
          // cout << "      mypid " << mypid << " is already in sg " << tmpsgid << endl;
          action = -1;
          break;
        }
        if (tmpsg->childsMatch(myChildIds)) {
          // cout << "      mypid " << mypid << " can be added in sg " << tmpsgid << endl;
          action = 1;
          addsgid = tmpsgid;
          break;
        }
      }

      if (action == 0) {
        // we checked all the sg for all the child and do not find a match, so we just create a new sg
        int sgid = _sgId++;
        SimpleGraph* sg = new SimpleGraph(sgid, myChildIds, mypid, myChildCoefs);
        _sgMap.insert(make_pair(sgid, sg));
        _sgList.push_back(sgid);

        // cout << "    create a new sg: " << sg->dumpStr() << endl;
        // mark child2sg
        for (auto cid: myChildIds) {
          if (_child2sgs.find(cid) == _child2sgs.end()) {
            vector<int> tmplist = {sgid};
            _child2sgs.insert(make_pair(cid, tmplist));
          } else
            _child2sgs[cid].push_back(sgid);
        }

        // decrease number of child in refMap
        refMap[mypid]-=myChildIds.size();
        if (refMap[mypid] == 0) {
          // cout << "    add " << mypid << " into leaves" << endl;
          leaves.push(mypid);
        }
        
        // mark nodeId as process
        process = true;
      } else if (action == 1) {
        SimpleGraph* tmpsg = _sgMap[addsgid];
        tmpsg->addParent(mypid, myCoefMap);
        // cout << "    update sg: " << tmpsg->dumpStr() << endl;

        // decrease number of child in refMap
        refMap[mypid]-=myChildIds.size();
        if (refMap[mypid] == 0) {
          // cout << "    add " << mypid << " into leaves" << endl;
          leaves.push(mypid);
        }
        
        // mark nodeId as process
        process = true;
      } else if (action == -1) {
        process = true;
      } 
    }
    if (!process) {
      // we fail to process nodeId at this time
      leaves.push(nodeId);
      // cout << "    put " << nodeId << " back into the queue for further process" << endl;
    }
    debugnum++;
    //if (debugnum == 22)
    //  break;
  }
}

void ECDAG::initializeLeaveIp(unordered_map<int, unsigned int> availIps) {
  for (auto item: availIps) {
    int idx = item.first;
    unsigned int ip = item.second;
    if (_ecNodeMap.find(idx) != _ecNodeMap.end()) {
      _idx2ip.insert(make_pair(idx, ip));
    }    
  }
}

void ECDAG::initializeRequestorIp(unsigned int ip) {
  _idx2ip.insert(make_pair(REQUESTOR, ip));
}

void ECDAG::initializeIntermediateIp(unsigned int ip) {
  for (auto item: _ecNodeMap) {
    int nodeId = item.first;
    if (_idx2ip.find(nodeId) == _idx2ip.end()) {
      _idx2ip.insert(make_pair(nodeId, ip)); 
    }
  }
}

void ECDAG::fillLoadVector(LoadVector* loadvector) {
  // we check each sg iteratively and fill in the load vector accordingly.
  for (int i=0; i<_sgList.size(); i++) {
    int sgid = _sgList[i];
    SimpleGraph* sg = _sgMap[sgid];
    vector<int> childIdx = sg->getChilds();
    vector<int> parentIdx = sg->getParents();

    // check whether parent have the same color
    bool psame = true;
    if (parentIdx.size() == 1)
      psame = false;
    for (int j=0; j<parentIdx.size()-1; j++) {
      if (_idx2ip[parentIdx[j]] != _idx2ip[parentIdx[j+1]]) {
        psame = false;
        break;
      }
    }

    if (psame) {
      unsigned int pip = _idx2ip[parentIdx[0]];
      for (auto cid: childIdx) {
        unsigned int cip = _idx2ip[cid];
        if (pip != cip) {
          loadvector->updateOutValueFor(cip, 1);
          loadvector->updateInValueFor(pip, 1);
        }
      }
      loadvector->updateCalValueFor(pip, childIdx.size() * parentIdx.size());
    } else {
      for (int pid: parentIdx) {
        unsigned int pip = _idx2ip[pid];
        for (int cid: childIdx) {
          unsigned int cip = _idx2ip[cid];
          if (pip != cip) {
            loadvector->updateOutValueFor(cip, 1);
            loadvector->updateInValueFor(pip, 1);
          } 
        }
        loadvector->updateCalValueFor(pip, childIdx.size());
      }
    }
  }
}

void ECDAG::coloring(LoadVector* loadvector) {
  
  // generate a map that marks whether a node is colored
  // key does not exist, not colored
  // value = 0, not colored
  // value = 1, colored
  unordered_map<int, int> nodeColorMap;

  cout << "ECDAG::coloring:leaves: ";
  for (int nodeId: _ecLeaves) {
    cout << nodeId << " ";
  }
  cout << endl;
   
  // we mark leave nodes as colored
  for (int nodeId: _ecLeaves) {
    nodeColorMap.insert(make_pair(nodeId, 1));
  }

  // we mark root node as colored
  nodeColorMap.insert(make_pair(REQUESTOR, 1));
  cout << "colorMap before coloring: " << endl;
  for (auto item: nodeColorMap) {
    if (nodeColorMap[item.first] == 1)
    cout << "  " << item.first << ": " << RedisUtil::ip2Str(_idx2ip[item.first]) << endl;
  }
  cout << endl;

  // check each sg iteratively
  int debugnum = 0;
  for (auto sgid: _sgList) {
    debugnum++;

    SimpleGraph* sg = _sgMap[sgid];
    // cout << "check sg: " << sg->dumpStr() << endl;

    vector<int> childIdList = sg->getChilds();
    vector<int> parentIdList = sg->getParents();

    // we need to make sure that the current sg can be colored!
    for (auto childId: childIdList) {
      assert(nodeColorMap.find(childId) != nodeColorMap.end() && nodeColorMap[childId] == 1);
    }

    // we check whether all parents have been colored
    bool pcolored = true;
    for (auto parentId: parentIdList) {
      if (nodeColorMap.find(parentId) == nodeColorMap.end()) {
        pcolored = false;
        break;
      }
    }

    if (pcolored) {
      // this sg has been colored
      cout << "  This sg has been colored! Skip coloring this sg!" << endl;
      continue;
    }

    // check whether all the parent are in the same initial color
    bool psame = true;
    if (parentIdList.size() == 1)
      psame = false;
    else {
      for (int i=0; i<parentIdList.size()-1; i++) {
        if (_idx2ip[parentIdList[i]] != _idx2ip[parentIdList[i+1]]) {
          psame = false;
          break;
        }
      }
    }
  
    // if all parents have the same color, consider them as a integrity
    if (psame) {
      unsigned int colora = _idx2ip[parentIdList[0]];
      cout << "  all the parent are in the same color " << RedisUtil::ip2Str(colora) << endl;

      // list all color of childs (colorb) in candidates
      vector<unsigned int> candidates;
      for (auto cid: childIdList) {
        if (find(candidates.begin(), candidates.end(), _idx2ip[cid]) == candidates.end())
          candidates.push_back(_idx2ip[cid]);
      }

      // try each color b to obtain (traffic, compute) overhead
      unsigned int minColor = 0;
      LoadVector* minLoadVector= new LoadVector(loadvector->getNodeList());

      for (auto colorb: candidates) {

        // duplicate a loadvector
        LoadVector* tmploadvector = new LoadVector(loadvector->getNodeList(), 
                                                   loadvector->getInNum(),
                                                   loadvector->getOutNum(),
                                                   loadvector->getCalNum()); 
        cout << "    try " << RedisUtil::ip2Str(colorb) << endl; 
        // we first re-calculate the overhead of the child nodes in current SG
        for (auto cid: childIdList) {
          unsigned int colorc = _idx2ip[cid];
          cout << "      check child " << cid << ", colorc: " << RedisUtil::ip2Str(colorc) << ", parent change colora: " << RedisUtil::ip2Str(colora) << " -> color b: " << RedisUtil::ip2Str(colorb) << endl;
          if (colorc != colora) {
            tmploadvector->updateOutValueFor(colorc, -1);
            tmploadvector->updateInValueFor(colora, -1);
            cout << "        colorc: " << RedisUtil::ip2Str(colorc) << "    outNum -1" << endl;
            cout << "        colora: " << RedisUtil::ip2Str(colora) << "    inNum -1" << endl;
          }

          if (colorc != colorb) {
            tmploadvector->updateOutValueFor(colorc, 1);
            tmploadvector->updateInValueFor(colorb, 1);
            cout << "        colorc: " << RedisUtil::ip2Str(colorc) << "    outNum +1" << endl;
            cout << "        colorb: " << RedisUtil::ip2Str(colorb) << "    inNum +1" << endl;
          }
        }
        int computev = childIdList.size() * parentIdList.size();
        tmploadvector->updateCalValueFor(colora, -1 * computev);
        tmploadvector->updateCalValueFor(colorb, computev);
        cout << "      colora: " << RedisUtil::ip2Str(colora) << "    calNum - " << computev << endl;
        cout << "      colorb: " << RedisUtil::ip2Str(colorb) << "    calNum + " << computev << endl;
        // Secondly, we re-calculate the overhead for all parent SG of the current SG
        for (auto pid: parentIdList) {
          vector<int> psgIdList = _child2sgs[pid];
          cout << "      check parentSGlist of " << pid << ": ";
          for (auto psgid: psgIdList)
            cout << psgid << " ";
          cout << endl;

          for (auto psgid: psgIdList) {
            cout << "        check psgid " << psgid << endl;
            // check whther all parents in this psgid
            SimpleGraph* tmpsg = _sgMap[psgid];
            vector<int> tmpParentList = tmpsg->getParents(); 

            bool tmpsame = true;
            if (tmpParentList.size() == 1)
              tmpsame = false;
            else {
              for (int tmpi=0; tmpi<tmpParentList.size()-1; tmpi++) {
                if (_idx2ip[tmpParentList[tmpi]] != _idx2ip[tmpParentList[tmpi+1]])
                  tmpsame = false;
              }
            }

            if (tmpsame) {
              unsigned int colord = _idx2ip[tmpParentList[0]];
              cout << "          all parent are in the same colord: " << RedisUtil::ip2Str(colord) << endl;
              cout << "            check colord: " << RedisUtil::ip2Str(colord) << ", child change colora: " << RedisUtil::ip2Str(colora) << " -> color b: " << RedisUtil::ip2Str(colorb) << endl;
              if (colora != colord) {
                tmploadvector->updateOutValueFor(colora, -1);
                tmploadvector->updateInValueFor(colord, -1);
                cout << "              colora: " << RedisUtil::ip2Str(colora) << "    outNum -1" << endl;
                cout << "              colord: " << RedisUtil::ip2Str(colord) << "    inNum -1" << endl;
              }

              if (colorb != colord) {
                tmploadvector->updateOutValueFor(colorb, 1);
                tmploadvector->updateInValueFor(colord, 1);
                cout << "              colorb: " << RedisUtil::ip2Str(colorb) << "    outNum +1" << endl;
                cout << "              colord: " << RedisUtil::ip2Str(colord) << "    inNum +1" << endl;
              }
            } else {
              for (auto tmppid: tmpParentList) {
                unsigned int colord = _idx2ip[tmppid];
                cout << "          check tmppid " << tmppid << ", colord: " << RedisUtil::ip2Str(colord) << ", child change colora: " << RedisUtil::ip2Str(colora) << " -> color b: " << RedisUtil::ip2Str(colorb) << endl;
              
                if (colora != colord) {
                  tmploadvector->updateOutValueFor(colora, -1);
                  tmploadvector->updateInValueFor(colord, -1);
                  cout << "              colora: " << RedisUtil::ip2Str(colora) << "    outNum -1" << endl;
                  cout << "              colord: " << RedisUtil::ip2Str(colord) << "    inNum -1" << endl;
                }  

                if (colorb != colord) {
                  tmploadvector->updateOutValueFor(colorb, 1);
                  tmploadvector->updateInValueFor(colord, 1);
                  cout << "              colorb: " << RedisUtil::ip2Str(colorb) << "    outNum +1" << endl;
                  cout << "              colord: " << RedisUtil::ip2Str(colord) << "    inNum +1" << endl;
                }
              }
            }
          }
        } // end check all parent sgs for the current sg
        cout << "      after trying, the loadvector is " << tmploadvector->dumpStr() << endl;
        
        if (minColor == 0) {
          // minLoadVector hasn't been intialized
          minColor = colora;
          minLoadVector->setLoadValue(loadvector->getInNum(),
                                      loadvector->getOutNum(),
                                      loadvector->getCalNum());
        }

        // we need to compare tmploadvector with minLoadVector to see which one is more 
        cout << "      loada: " << minLoadVector->dumpStr() << endl;
        cout << "      loadb: " << tmploadvector->dumpStr() << endl;
        int res = LoadVector::compare(minLoadVector, tmploadvector);

        if (res == -1) {
          // loadb is better, we update minwith loadb
          minColor = colorb;
          minLoadVector->setLoadValue(tmploadvector->getInNum(),
                                      tmploadvector->getOutNum(),
                                      tmploadvector->getCalNum());
        }
        cout << "      now, minColor: " << RedisUtil::ip2Str(minColor) << endl;
        pair<int, int> minoh = minLoadVector->getOverhead();
        cout << "      now, minLoad: ( " << to_string(minoh.first) << ", " << to_string(minoh.second) << " )" << endl;

        //break;
      }  // finish try all the child color 

      // apply minLoadVector to loadvector
      loadvector->setLoadValue(minLoadVector->getInNum(),
                               minLoadVector->getOutNum(),
                               minLoadVector->getCalNum()); 
      cout << "    apply " << RedisUtil::ip2Str(minColor) << " to all the parents of sg " << sgid << endl;
      for (auto tmppid: parentIdList) {
        _idx2ip[tmppid] = minColor;
        cout << "      tmppid: " << tmppid << ", color: " << RedisUtil::ip2Str(_idx2ip[tmppid]) << endl;
        nodeColorMap.insert(make_pair(tmppid, 1));
      }
      cout << "    loadvector: " << loadvector->dumpStr() << endl;
    } else {
      // all parents does not have the same color
      cout << "  parents are in different colors" << endl;
      for (auto pid: parentIdList) {
         cout << "    pid: " << pid << ", color: " << RedisUtil::ip2Str(_idx2ip[pid]) << endl;
      }

      for (auto pid: parentIdList) {
        // for each parent, list all the child
        
        // the current colora
        unsigned int colora = _idx2ip[pid];

        // list all color of childs (colorb) in candidates
        vector<unsigned int> candidates;
        for (auto cid: childIdList) {
          if (find(candidates.begin(), candidates.end(), _idx2ip[cid]) == candidates.end())
            candidates.push_back(_idx2ip[cid]);
        }

        // try each color b to obtain (traffic, compute) overhead
        unsigned int minColor = 0;
        LoadVector* minLoadVector= new LoadVector(loadvector->getNodeList());

        for (auto colorb: candidates) {

          // duplicate a loadvector
          LoadVector* tmploadvector = new LoadVector(loadvector->getNodeList(), 
                                                     loadvector->getInNum(),
                                                     loadvector->getOutNum(),
                                                     loadvector->getCalNum()); 
          cout << "    try " << RedisUtil::ip2Str(colorb) << endl; 
          // we first re-calculate the overhead of the child nodes in current SG
          for (auto cid: childIdList) {
            unsigned int colorc = _idx2ip[cid];
            cout << "      check child " << cid << ", colorc: " << RedisUtil::ip2Str(colorc) << ", parent change colora: " << RedisUtil::ip2Str(colora) << " -> color b: " << RedisUtil::ip2Str(colorb) << endl;
            if (colorc != colora) {
              tmploadvector->updateOutValueFor(colorc, -1);
              tmploadvector->updateInValueFor(colora, -1);
              cout << "        colorc: " << RedisUtil::ip2Str(colorc) << "    outNum -1" << endl;
              cout << "        colora: " << RedisUtil::ip2Str(colora) << "    inNum -1" << endl;
            }

            if (colorc != colorb) {
              tmploadvector->updateOutValueFor(colorc, 1);
              tmploadvector->updateInValueFor(colorb, 1);
              cout << "        colorc: " << RedisUtil::ip2Str(colorc) << "    outNum +1" << endl;
              cout << "        colorb: " << RedisUtil::ip2Str(colorb) << "    inNum +1" << endl;
            }
          }
          int computev = childIdList.size() * parentIdList.size();
          tmploadvector->updateCalValueFor(colora, -1 * computev);
          tmploadvector->updateCalValueFor(colorb, computev);
          cout << "      colora: " << RedisUtil::ip2Str(colora) << "    calNum - " << computev << endl;
          cout << "      colorb: " << RedisUtil::ip2Str(colorb) << "    calNum + " << computev << endl;

          // Secondly, we re-calculate the overhead for all parent SG of the current SG
          for (auto pid: parentIdList) {
            vector<int> psgIdList = _child2sgs[pid];
            cout << "      check parentSGlist of " << pid << ": ";
            for (auto psgid: psgIdList)
              cout << psgid << " ";
            cout << endl;

            for (auto psgid: psgIdList) {
              cout << "        check psgid " << psgid << endl;
              // check whther all parents in this psgid
              SimpleGraph* tmpsg = _sgMap[psgid];
              vector<int> tmpParentList = tmpsg->getParents(); 

              bool tmpsame = true;
              if (tmpParentList.size() == 1)
                tmpsame = false;
              else {
                for (int tmpi=0; tmpi<tmpParentList.size()-1; tmpi++) {
                  if (_idx2ip[tmpParentList[tmpi]] != _idx2ip[tmpParentList[tmpi+1]])
                    tmpsame = false;
                }
              }

              if (tmpsame) {
                unsigned int colord = _idx2ip[tmpParentList[0]];
                cout << "          all parent are in the same colord: " << RedisUtil::ip2Str(colord) << endl;
                cout << "            check colord: " << RedisUtil::ip2Str(colord) << ", child change colora: " << RedisUtil::ip2Str(colora) << " -> color b: " << RedisUtil::ip2Str(colorb) << endl;
                if (colora != colord) {
                  tmploadvector->updateOutValueFor(colora, -1);
                  tmploadvector->updateInValueFor(colord, -1);
                  cout << "              colora: " << RedisUtil::ip2Str(colora) << "    outNum -1" << endl;
                  cout << "              colord: " << RedisUtil::ip2Str(colord) << "    inNum -1" << endl;
                }

                if (colorb != colord) {
                  tmploadvector->updateOutValueFor(colorb, 1);
                  tmploadvector->updateInValueFor(colord, 1);
                  cout << "              colorb: " << RedisUtil::ip2Str(colorb) << "    outNum +1" << endl;
                  cout << "              colord: " << RedisUtil::ip2Str(colord) << "    inNum +1" << endl;
                }
              } else {
                for (auto tmppid: tmpParentList) {
                  unsigned int colord = _idx2ip[tmppid];
                  cout << "          check tmppid " << tmppid << ", colord: " << RedisUtil::ip2Str(colord) << ", child change colora: " << RedisUtil::ip2Str(colora) << " -> color b: " << RedisUtil::ip2Str(colorb) << endl;
                
                  if (colora != colord) {
                    tmploadvector->updateOutValueFor(colora, -1);
                    tmploadvector->updateInValueFor(colord, -1);
                    cout << "              colora: " << RedisUtil::ip2Str(colora) << "    outNum -1" << endl;
                    cout << "              colord: " << RedisUtil::ip2Str(colord) << "    inNum -1" << endl;
                  }  

                  if (colorb != colord) {
                    tmploadvector->updateOutValueFor(colorb, 1);
                    tmploadvector->updateInValueFor(colord, 1);
                    cout << "              colorb: " << RedisUtil::ip2Str(colorb) << "    outNum +1" << endl;
                    cout << "              colord: " << RedisUtil::ip2Str(colord) << "    inNum +1" << endl;
                  }
                }
              }
            }
          } // end check all parent sgs for the current sg

          cout << "      after trying, the loadvector is " << tmploadvector->dumpStr() << endl;

          if (minColor == 0) {
            // minLoadVector hasn't been intialized
            minColor = colora;
            minLoadVector->setLoadValue(loadvector->getInNum(),
                                        loadvector->getOutNum(),
                                        loadvector->getCalNum());
          }

          // we need to compare tmploadvector with minLoadVector to see which one is more 
          cout << "      loada: " << minLoadVector->dumpStr() << endl;
          cout << "      loadb: " << tmploadvector->dumpStr() << endl;
          int res = LoadVector::compare(minLoadVector, tmploadvector);

          if (res == -1) {
            // loadb is better, we update minwith loadb
            minColor = colorb;
            minLoadVector->setLoadValue(tmploadvector->getInNum(),
                                        tmploadvector->getOutNum(),
                                        tmploadvector->getCalNum());
          }
          cout << "      now, minColor: " << RedisUtil::ip2Str(minColor) << endl;
          pair<int, int> minoh = minLoadVector->getOverhead();
          cout << "      now, minLoad: ( " << to_string(minoh.first) << ", " << to_string(minoh.second) << " )" << endl;
        } // finish try all the child color
        // apply minLoadVector to loadvector
        loadvector->setLoadValue(minLoadVector->getInNum(),
                                 minLoadVector->getOutNum(),
                                 minLoadVector->getCalNum()); 
        cout << "    apply " << RedisUtil::ip2Str(minColor) << " to all the parents of sg " << sgid << endl;
        for (auto tmppid: parentIdList) {
          _idx2ip[tmppid] = minColor;
          cout << "      tmppid: " << tmppid << ", color: " << RedisUtil::ip2Str(_idx2ip[tmppid]) << endl;
          nodeColorMap.insert(make_pair(tmppid, 1));
        }
        cout << "    loadvector: " << loadvector->dumpStr() << endl;
      }
    }

    if (debugnum == 10)
      break;
  } 
}

void ECDAG::genECTasks(vector<ECTask*>& tasklist,
                       int ecn, int eck, int ecw,
                       string stripename, vector<string> blocklist) {

//  // we check each sg iteratively, and gen commands for each sg
//  int debugnum = 0;
//  for (int sgidx=0; sgidx<_sgList.size(); sgidx++) {
//    int sgid = _sgList[sgidx];
//    SimpleGraph* sg = _sgMap[sgid];
//    // cout << "check sg: " << sg->dumpStr() << endl;
//
//    vector<int> childIdList = sg->getChilds();
//    vector<int> parentIdList = sg->getParents();
//    unordered_map<int, vector<int>> coefs = sg->getCoefs();
//    
//    // We first check whether we have leave nodes in the child list
//    // If there are leave nodes, we put them into buckets of blocks
//    unordered_map<int, vector<int>> blk2pkts;
//    for (auto childId: childIdList) {
//      if (find(_ecLeaves.begin(), _ecLeaves.end(), childId) != _ecLeaves.end()) {
//        // this is a leave node
//        int blkIdx = childId/ecw;
//        if (blk2pkts.find(blkIdx) == blk2pkts.end()) {
//          vector<int> tmplist = {childId};
//          blk2pkts.insert(make_pair(blkIdx, tmplist));
//        } else {
//          blk2pkts[blkIdx].push_back(childId);
//        }
//      }
//    }
//
//    // if there are leave nodes in this sg, we create ectasks to read from disk
//    for (auto item: blk2pkts) {
//      int blkidx = item.first;
//      vector<int> indices = blk2pkts[blkidx];
//
//      int type = 0;
//      unsigned int loc = _idx2ip[indices[0]];
//      string blockname = blocklist[blkidx];
//      ECTask* readTask = new ECTask();   
//      readTask->buildReadDisk(type, loc, blockname, ecw, indices, stripename);
//      tasklist.push_back(readTask);
//    }
//
//    // if parent contains root node, we create ectasks to concact data 
//    if (parentIdList.size() == 1 && parentIdList[0] == REQUESTOR) {
//      cout << "create task for requestor!!!!!!!!!!" << endl;
//      // we need to concatenate for the requestor
//      ECTask* concatenateTask = new ECTask();
//      int type = 2;
//      unsigned int loc = _idx2ip[parentIdList[0]];
//      vector<unsigned int> prevlocs;
//      for (auto item: childIdList)
//        prevlocs.push_back(_idx2ip[item]);
//      int blkidx = childIdList[0] / ecw;
//      string blockname = blocklist[blkidx];
//      
//      concatenateTask->buildConcatenate(type, loc, childIdList, prevlocs, stripename, blockname, ecw);
//
//      tasklist.push_back(concatenateTask);
//    } else {
//      // we create ectasks to fetchandcompute
//      ECTask* fetchComputeTask = new ECTask();
//      int type = 1;
//      unsigned int loc = _idx2ip[parentIdList[0]];
//      vector<unsigned int> prevlocs;
//      for (auto item: childIdList)
//        prevlocs.push_back(_idx2ip[item]);
//
//      vector<ComputeTask*> ctlist;
//      vector<vector<int>> tmpcoef;
//      for (int pid: parentIdList) {
//        tmpcoef.push_back(coefs[pid]);
//      }
//      ComputeTask* ct = new ComputeTask(childIdList, parentIdList, tmpcoef);
//      ctlist.push_back(ct);
//
//      fetchComputeTask->buildFetchCompute(type, loc, childIdList, prevlocs, ctlist, stripename, parentIdList, ecw); 
//
//      tasklist.push_back(fetchComputeTask);
//    }
//
//    debugnum++;
//    //if (debugnum == 1)
//    //  break;
//  }
}

void ECDAG::genECTasksByECClusters(vector<ECTask*>& tasklist,                                                                                                                         
        int ecn, int eck, int ecw, int blkbytes, int pktbytes, 
        string stripename, vector<string> blocklist,
        unordered_map<int, unsigned int> coloring_res) {
    cout << "ECUnits: " << endl;
    for (int i=0; i<_ecUnitList.size(); i++) {
        int unitId = _ecUnitList[i];
        ECUnit* cunit = _ecUnitMap[unitId];
        cout << "  " << cunit->dump() << endl;
    }

    // note, if a leaf vertex is a virtual block, we ignore it.
    unordered_map<int, int> leaveRefs;
    for (auto cid: _ecLeaves) {
        int bidx = cid/ecw;
        if (bidx < ecn)
            leaveRefs.insert(make_pair(cid, 0));
    }

    cout << "ECCluster:" << endl;
    for (int i=0; i<_ecClusterList.size(); i++) {
        int clusterId = _ecClusterList[i];
        ECCluster* ccluster = _ecClusterMap[clusterId];
        cout << "  " << ccluster->dump() << endl;

        // data structure for current cluster
        vector<int> childlist;
        unordered_map<int, bool> childmap;
        vector<int> parentlist;
        unordered_map<int, vector<int>> coefmap;

        vector<int> unitlist = ccluster->getUnitList();

        // figure out all the childs for the current cluster
        for (int j=0; j<unitlist.size(); j++) {
            int unitId = unitlist[j];
            ECUnit* cunit = _ecUnitMap[unitId];
            cout << "    unit: " << cunit->dump() << endl;

            vector<int> unitchilds = cunit->getChilds();
            for (auto cid: unitchilds) {
                if (childmap.find(cid) == childmap.end()) {
                    childmap.insert(make_pair(cid, true));
                    childlist.push_back(cid);
                }
            }
        }
        sort(childlist.begin(), childlist.end());

        // update ref in leaveRefs
        for (auto cid: childlist) {
            if (leaveRefs.find(cid)!=leaveRefs.end())
                leaveRefs[cid]++;
        }
        
        cout << "    childlist: ";
        for (int tmpi=0; tmpi < childlist.size(); tmpi++) {
            cout << childlist[tmpi] << " ";
        }
        cout << endl;

        // update coefs for childlist
        for (int j=0; j<unitlist.size(); j++) {
            int unitId = unitlist[j];
            ECUnit* cunit = _ecUnitMap[unitId];
            cout << "    unit: " << cunit->dump() << endl;
            vector<int> unitchilds = cunit->getChilds();
            vector<int> unitcoefs = cunit->getCoefs();
            int parent = cunit->getParent();

            cout << "    parent: " << parent << ", childs: ";
            for (int tmpi=0; tmpi<unitchilds.size(); tmpi++)
                cout << unitchilds[tmpi] << " ";
            cout << ", coefs: ";
            for (int tmpi=0; tmpi<unitcoefs.size(); tmpi++)
                cout << unitcoefs[tmpi] << " ";
            cout << endl;

            vector<int> coeflist(childlist.size());
            for (int tmpi=0; tmpi<childlist.size(); tmpi++) {
                int cid = childlist[tmpi];
                int c = 0;
                for (int tmpj=0; tmpj<unitchilds.size(); tmpj++) {
                    if (cid == unitchilds[tmpj])
                        c = unitcoefs[tmpj];
                }
                coeflist[tmpi] = c;
            }
            
            cout << "    parent: " << parent << ", newchilds: ";
            for (int tmpi=0; tmpi<childlist.size(); tmpi++)
                cout << childlist[tmpi] << " ";
            cout << ", newcoefs: ";
            for (int tmpi=0; tmpi<coeflist.size(); tmpi++)
                cout << coeflist[tmpi] << " ";
            cout << endl;

            parentlist.push_back(parent);
            coefmap.insert(make_pair(parent, coeflist));
        }

        // setup data structures in cluster
        ccluster->setChildList(childlist);
        ccluster->setParentList(parentlist);
        ccluster->setCoefMap(coefmap);
    }

    // check leaveRefs, merge cid of the same block
    unordered_map<int, vector<int>> bid2cids;
    for (auto item: leaveRefs) {
        int cid = item.first;
        int bid = cid/ecw;
        if (bid2cids.find(bid) == bid2cids.end()) {
            vector<int> list;
            list.push_back(cid);
            bid2cids.insert(make_pair(bid, list));
        } else {
            bid2cids[bid].push_back(cid);
        }
    }
    // buildReadDisk
    for (auto item: bid2cids) {
        int bid = item.first;
        vector<int> cidlist = item.second;
        sort(cidlist.begin(), cidlist.end());

        int type = 0;
        unsigned int loc = coloring_res[cidlist[0]];
        string blockname = blocklist[bid];
        unordered_map<int, int> cid2refs;
        for (auto cid: cidlist) {
            int r = leaveRefs[cid];
            cid2refs.insert(make_pair(cid, r));
        }

        ECTask* readDiskTask = new ECTask();
        readDiskTask->buildReadDisk(type, loc, blockname, blkbytes, pktbytes, ecw, cid2refs, stripename);
        tasklist.push_back(readDiskTask);
    }
    // build fetchCompute and Concact
    for (int i=0; i<_ecClusterList.size(); i++) {
        int clusterId = _ecClusterList[i];
        ECCluster* ccluster = _ecClusterMap[clusterId];
        cout << "cluster: " << clusterId << endl;
        vector<int> childlist = ccluster->getChildList();
        vector<int> parentlist = ccluster->getParentList();
        unordered_map<int, vector<int>> coefmap = ccluster->getCoefMap();

        unsigned int loc = coloring_res[parentlist[0]];

        vector<unsigned int> prevlocs;
        for (int tmpi=0; tmpi<childlist.size(); tmpi++) {
            int cid = childlist[tmpi];
            unsigned int tmpl = coloring_res[cid];
            prevlocs.push_back(tmpl);
        }
        

        if (ccluster->isConcact(REQUESTOR)) {
            cout << "  Concact!" << endl;

            int type = 2;
            int bidx = childlist[0] / ecw;
            string blockname = blocklist[bidx];

            ECTask* concactTask = new ECTask();
            concactTask->buildConcatenate(type, loc, childlist, prevlocs, stripename, blockname, ecw, blkbytes, pktbytes);
            tasklist.push_back(concactTask);

        } else {
            cout << "  FetchCompute!" << endl;
            vector<int> childlist = ccluster->getChildList();
            vector<int> parentlist = ccluster->getParentList();
            unordered_map<int, vector<int>> coefmap = ccluster->getCoefMap();

            int type = 1;
            
            vector<ComputeTask*> computelist;
            vector<vector<int>> coeflist;
            for (int tmpi=0; tmpi<parentlist.size(); tmpi++) {
                int pid = parentlist[tmpi];
                vector<int> coefs = coefmap[pid];
                coeflist.push_back(coefs);
            }
            ComputeTask* ct = new ComputeTask(childlist, parentlist, coeflist);
            computelist.push_back(ct);

            ECTask* fetchComputeTask = new ECTask();
            fetchComputeTask->buildFetchCompute(type, loc, childlist, prevlocs, 
                    computelist, stripename, parentlist, ecw, blkbytes, pktbytes);
            tasklist.push_back(fetchComputeTask);
        }
    }
}

void ECDAG::simulateLoc() {
  for (auto item: _ecNodeMap) {
    int idx = item.first;
    _idx2ip.insert(make_pair(idx, 0));
  }
}

unordered_map<int, ECNode*> ECDAG::getECNodeMap() {
  return _ecNodeMap;
}

vector<int> ECDAG::getECHeaders() {
  return _ecHeaders;
}

vector<int> ECDAG::getECLeaves() {
  return _ecLeaves;
}

unordered_map<int, ECUnit*> ECDAG::getUnitMap() {
  return _ecUnitMap;
}

vector<int> ECDAG::getUnitList() {
  return _ecUnitList;
}

void ECDAG::dump() {
  for (auto id : _ecHeaders) {
    _ecNodeMap[id] ->dump(-1);
    cout << endl;
  }
//  for (auto cluster: _clusterMap) {
//    cluster->dump();
//  }
}
