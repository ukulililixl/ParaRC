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
  // debug start
  string msg = "ECDAG::Join(" + to_string(pidx) + ",";
  for (int i=0; i<cidx.size(); i++) msg += " "+to_string(cidx[i]);
  msg += ",";
  for (int i=0; i<coefs.size(); i++) msg += " "+to_string(coefs[i]);
  msg += ")";
  if (ECDAG_DEBUG_ENABLE) cout << msg << endl;
  // debug end

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

void ECDAG::genSimpleGraph() {
  
  // 0. generate references for each node
  unordered_map<int, int> refMap;
  for (auto item: _ecNodeMap) {
    int nodeId = item.first;
    ECNode* node = item.second;
    refMap.insert(make_pair(nodeId, node->getNumChilds()));
  }
  cout << "ECDAG::genSimpleGraph.refMap: " << endl;
  for (auto item: refMap) {
    cout << "(" << item.first << ", " << item.second << ") ";
  }
  cout << endl;

  // 1. Each time we search for leaf nodes
  BlockingQueue<int> leaves;
  for (auto item: refMap) {
    if (item.second == 0)
      leaves.push(item.first);
  }    

  int debugnum = 0;
  while (leaves.getSize() > 0) {
    int nodeId = leaves.pop();
    cout << "Check node " << nodeId << endl;

    // get all parent nodes for this node
    ECNode* curNode = _ecNodeMap[nodeId];
    vector<ECNode*> parentNodes = curNode->getParentNodes();
    if (parentNodes.size() == 0) {
      cout << "Reach the root, exit!" << endl;
      break;
    }

    // we first mark it as process, if we cannot process it as this time, mark it as false and put it into the queue 
    bool process = false;
    // for each parent, check whether its childs are already in a cluster 
    for (auto pitem: parentNodes) {
      int mypid = pitem->getNodeId();
      cout << "  mypid: " << mypid << endl;

      // if pid' child number is 0, then we have processed it
      if (refMap[mypid] == 0) {
        cout << "    mypid " << mypid << " has been processed!" << endl;
        process = true;
        continue;
      }
      
      // get all the childIds
      vector<ECNode*> myChildNodes = pitem->getChildNodes();
      vector<int> myChildIds;
      for (auto citem: myChildNodes) 
        myChildIds.push_back(citem->getNodeId());
      cout << "    myChildIds: ";
      for (auto cid: myChildIds)
        cout << cid << " ";
      cout << endl;

      // check whether each child is a leaf node
      bool proceed = true;
      for (auto cid: myChildIds) {
        if (refMap[cid] != 0) {
          cout << "    child " << cid << " is not a leaf! cannot proceed with parentid " << mypid << endl;
          proceed = false;
          break;
        }
      }
      if (!proceed) {
        // continue to check the next parent
        continue;
      }

      cout << "    all the childs are leaf nodes, we can proceed with parent " << mypid << endl;

      // get coefs for childs
      vector<int> myChildCoefs = pitem->getCoefs();
      unordered_map<int, int> myCoefMap;
      for (int i=0; i<myChildIds.size(); i++) {
        myCoefMap.insert(make_pair(myChildIds[i], myChildCoefs[i]));
      }
 
      cout << "    myChildCoefs: ";
      for (auto c: myChildCoefs)
        cout << c << " ";
      cout << endl;

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

      cout << "    candidatesgids: ";
      for (int tmpsgid: candidatesgids)
        cout << tmpsgid << " ";
      cout << endl;

      // now for each sgid, we check whether the current myChildIds can fit into it.
      // if yes, we add the current parent into that sg
      // if not, we create new sg
      int action = 0; // 0: create; 1: add; -1: do nothing
      int addsgid = -1;
      for (int tmpsgid: candidatesgids) {
        SimpleGraph* tmpsg = _sgMap[tmpsgid];
        if (tmpsg->parentIn(mypid)) {
          cout << "      mypid " << mypid << " is already in sg " << tmpsgid << endl;
          action = -1;
          break;
        }
        if (tmpsg->childsMatch(myChildIds)) {
          cout << "      mypid " << mypid << " can be added in sg " << tmpsgid << endl;
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

        cout << "    create a new sg: " << sg->dumpStr() << endl;
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
          cout << "    add " << mypid << " into leaves" << endl;
          leaves.push(mypid);
        }
        
        // mark nodeId as process
        process = true;
      } else if (action == 1) {
        SimpleGraph* tmpsg = _sgMap[addsgid];
        tmpsg->addParent(mypid, myCoefMap);
        cout << "    update sg: " << tmpsg->dumpStr() << endl;

        // decrease number of child in refMap
        refMap[mypid]-=myChildIds.size();
        if (refMap[mypid] == 0) {
          cout << "    add " << mypid << " into leaves" << endl;
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
      cout << "    put " << nodeId << " back into the queue for further process" << endl;
    }
    debugnum++;
    if (debugnum == 22)
      break;
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
    cout << "check sg: " << sg->dumpStr() << endl;

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

  // we check each sg iteratively, and gen commands for each sg
  int debugnum = 0;
  for (int sgidx=0; sgidx<_sgList.size(); sgidx++) {
    int sgid = _sgList[sgidx];
    SimpleGraph* sg = _sgMap[sgid];
    cout << "check sg: " << sg->dumpStr() << endl;

    vector<int> childIdList = sg->getChilds();
    vector<int> parentIdList = sg->getParents();
    unordered_map<int, vector<int>> coefs = sg->getCoefs();
    
    // We first check whether we have leave nodes in the child list
    // If there are leave nodes, we put them into buckets of blocks
    unordered_map<int, vector<int>> blk2pkts;
    for (auto childId: childIdList) {
      if (find(_ecLeaves.begin(), _ecLeaves.end(), childId) != _ecLeaves.end()) {
        // this is a leave node
        int blkIdx = childId/ecw;
        if (blk2pkts.find(blkIdx) == blk2pkts.end()) {
          vector<int> tmplist = {childId};
          blk2pkts.insert(make_pair(blkIdx, tmplist));
        } else {
          blk2pkts[blkIdx].push_back(childId);
        }
      }
    }

    // if there are leave nodes in this sg, we create ectasks to read from disk
    for (auto item: blk2pkts) {
      int blkidx = item.first;
      vector<int> indices = blk2pkts[blkidx];

      int type = 0;
      unsigned int loc = _idx2ip[indices[0]];
      string blockname = blocklist[blkidx];
      ECTask* readTask = new ECTask();   
      readTask->buildReadDisk(type, loc, blockname, ecw, indices, stripename);
      tasklist.push_back(readTask);
    }

    // if parent contains root node, we create ectasks to concact data 
    if (parentIdList.size() == 1 && parentIdList[0] == REQUESTOR) {
      cout << "create task for requestor!!!!!!!!!!" << endl;
      // we need to concatenate for the requestor
      ECTask* concatenateTask = new ECTask();
      int type = 2;
      unsigned int loc = _idx2ip[parentIdList[0]];
      vector<unsigned int> prevlocs;
      for (auto item: childIdList)
        prevlocs.push_back(_idx2ip[item]);
      int blkidx = childIdList[0] / ecw;
      string blockname = blocklist[blkidx];
      
      concatenateTask->buildConcatenate(type, loc, childIdList, prevlocs, stripename, blockname, ecw);

      tasklist.push_back(concatenateTask);
    } else {
      // we create ectasks to fetchandcompute
      ECTask* fetchComputeTask = new ECTask();
      int type = 1;
      unsigned int loc = _idx2ip[parentIdList[0]];
      vector<unsigned int> prevlocs;
      for (auto item: childIdList)
        prevlocs.push_back(_idx2ip[item]);

      vector<ComputeTask*> ctlist;
      vector<vector<int>> tmpcoef;
      for (auto item: coefs) {
        tmpcoef.push_back(item.second);
      }
      ComputeTask* ct = new ComputeTask(childIdList, parentIdList, tmpcoef);
      ctlist.push_back(ct);

      fetchComputeTask->buildFetchCompute(type, loc, childIdList, prevlocs, ctlist, stripename, parentIdList, ecw); 

      tasklist.push_back(fetchComputeTask);
    }

    debugnum++;
    if (debugnum == 10)
      break;
  }
}

//int ECDAG::BindX(vector<int> idxs) {
//    if (idxs.size() <= 1) return -1;
//
//    // check whether the child are the same for each idx
//    bool childAligned = true;
//    
//    // we use a childRefs to record the number of references for each child
//    unordered_map<int, int> childRefs;
//    int oldnum = 0;
//    for (int i=0; i<idxs.size(); i++) {
//        int parentIdx = idxs[i];
//        assert(_ecNodeMap.find(parentIdx) != _ecNodeMap.end());
//        ECNode* curnode = _ecNodeMap[parentIdx];
//        vector<ECNode*> curchildnodes = curnode->getChildren();
//
//        for (int j=0; j<curchildnodes.size(); j++) {
//            int curNodeID = curchildnodes[j]->getNodeId();
//            if (childRefs.find(curNodeID) == childRefs.end())
//                childRefs[curNodeID] = 1;
//            else
//                childRefs[curNodeID] += 1;
//        }
//
//        if (oldnum == 0)
//            oldnum = childRefs.size();
//        else {
//            if (childRefs.size() != oldnum)
//                childAligned = false;
//        }
//    }
//
//    // now we check the number of references for each child
//    if (childAligned) {
//        int prev = -1;
//        for (auto item: childRefs) {
//            int childID = item.first;
//            int ref = item.second;
//            if (prev == -1)
//                prev = ref;
//            else {
//                if (ref != prev) {
//                    childAligned = false;
//                    break;
//                }
//            }
//        }
//    }
//
//    if (childAligned)
//        return BindXAligned(idxs);
//    else
//        return BindXNotAligned(idxs);
//}
//
//int ECDAG::BindXAligned(vector<int> idxs) {
//  if (idxs.size() <= 1) return -1;
//  // 0. create a bind node
//  int bindid = _bindId++;
//  assert (_ecNodeMap.find(bindid) == _ecNodeMap.end());
//  ECNode* bindNode = new ECNode(bindid);
//  // 1. we need to make sure for each node in idxs, their child are the same
//  vector<int> childids;
//  vector<ECNode*> childnodes; 
//  assert (idxs.size() > 0);
//  assert (_ecNodeMap.find(idxs[0]) != _ecNodeMap.end()); 
//  childnodes = _ecNodeMap[idxs[0]]->getChildren();
//  for (int i=0; i<childnodes.size(); i++) childids.push_back(childnodes[i]->getNodeId());
//  bindNode->setChilds(childnodes);
//  for (int i=1; i<idxs.size(); i++) {
//    assert(_ecNodeMap.find(idxs[i]) != _ecNodeMap.end());
//    ECNode* curnode = _ecNodeMap[idxs[i]];
//    vector<ECNode*> curchildnodes = curnode->getChildren();
//    assert (curchildnodes.size() == childids.size());
//    for (int j=0; j<curchildnodes.size(); j++) {
//      int curNodeID = curchildnodes[j]->getNodeId();
//      assert (find(childids.begin(), childids.end(), curNodeID) != childids.end());
//    }
//  }
//  // now we make sure that for each node in idxs, their child are the same
//  // childids contains the nodeIds, and childnodes contains the child nodes
//  // 2. for each node in idxs, figure out corresponding coef
//  for (int i=0; i<idxs.size(); i++) {
//    int tbid = idxs[i];
//    assert (_ecNodeMap.find(tbid) != _ecNodeMap.end());
//    ECNode* tbnode = _ecNodeMap[tbid];
//    // add the coef of cur node to the bind node
//    for (auto item: tbnode->getCoefmap()) {
//      bindNode->addCoefs(item.first, item.second);
//    }
//    // add the ref of cur node to the bind node
//    bindNode->setRefNum(tbid, tbnode->getRefNumFor(tbid));  // it seems no use here
//    // clean the childs of cur node and add bind node as child
//    tbnode->cleanChilds();
//    vector<ECNode*> newChildNodes = {bindNode};
//    tbnode->setChilds(newChildNodes);
//    tbnode->addCoefs(tbid, {1});
//  }
//  // 3. add tbnode into ecmap
//  _ecNodeMap.insert(make_pair(bindid, bindNode));
//  // 4. deal with cluster
//  int clusterid = findCluster(childids);
//  assert (clusterid != -1);
//  Cluster* cluster = _clusterMap[clusterid];
//  // TODO: set optimization?
//  cluster->setOpt(0); // BindX has opt level 0;
//
//  // update ref for childnodes?
//  for (int i=0; i<childnodes.size(); i++) {
//    int curcid = childids[i];
//    int curref = childnodes[i]->getRefNumFor(curcid);
//    childnodes[i]->setRefNum(curcid, curref-idxs.size()+1);
//  }
//  return bindid;
//}
//
//int ECDAG::BindXNotAligned(vector<int> idxs) {
//    cout << "ECDAG::BindXNotAligned" << endl;
//
//    // 0. figure out all the childs
//    unordered_map<int, int> childMap;
//    for (int i=0; i<idxs.size(); i++) {
//        int parentIdx = idxs[i];
//        cout << "parentIdx = " << parentIdx << endl;
//        assert(_ecNodeMap.find(parentIdx) != _ecNodeMap.end());
//        ECNode* curnode = _ecNodeMap[parentIdx];
//        vector<ECNode*> curchildnodes = curnode->getChildren();
//
//        vector<int> curchildlist;
//        for (int j=0; j<curchildnodes.size(); j++) {
//            int curNodeID = curchildnodes[j]->getNodeId();
//            curchildlist.push_back(curNodeID);
//            childMap[curNodeID] = 1;
//        }
//
//        // figure out corresponding child cluster
//        int childClusterID = findCluster(curchildlist);
//        assert(childClusterID != -1);
//        Cluster* childcluster = _clusterMap[childClusterID];
//        childcluster->removeParent(parentIdx);
//        childcluster->dump();
//    }
//    // now we have all the children in childMap
//  
//    // 1. create a bind node
//    int bindid = _bindId++;
//    assert (_ecNodeMap.find(bindid) == _ecNodeMap.end());
//    ECNode* bindNode = new ECNode(bindid);
//    
//    // 1. we set the child nodes for the bind node
//    vector<int> childids;
//    vector<ECNode*> childnodes; 
//    for (auto item: childMap) {
//        int childnodeID = item.first;
//        childids.push_back(childnodeID);
//        assert(_ecNodeMap.find(childnodeID) != _ecNodeMap.end());
//        ECNode* childnodeptr = _ecNodeMap[childnodeID];
//        childnodes.push_back(childnodeptr);
//    }
//    bindNode->setChilds(childnodes);
//
//    // 2. for each node in idxs, figure out corresponding coef
//    for (int i=0; i<idxs.size(); i++) {
//        int tbid = idxs[i];
//        assert (_ecNodeMap.find(tbid) != _ecNodeMap.end());
//        ECNode* tbnode = _ecNodeMap[tbid];
//
//        // get child list
//        vector<ECNode*> curchildren = tbnode->getChildren();
//        vector<int> curchildlist;
//        for (auto item: curchildren) {
//            int curchildid = item->getNodeId();
//            curchildlist.push_back(curchildid);
//        }
//
//        // get coefmap
//        unordered_map<int, vector<int>> curcoefmap = tbnode->getCoefmap();
//        unordered_map<int, vector<int>> bindcoefmap;
//        for (auto item: curcoefmap) {
//            int curparent = item.first;
//            vector<int> curcoeflist = item.second;
//            unordered_map<int, int> curidx2coef;
//            for (int tmpi=0; tmpi<curcoeflist.size(); tmpi++) {
//                int tmpidx = curchildlist[tmpi];
//                int tmpcoef = curcoeflist[tmpi];
//                curidx2coef[tmpidx]=tmpcoef;
//            }
//
//            // generate coef vector for childids
//            vector<int> bindcoef(childids.size());
//            for (int tmpi=0; tmpi<childids.size(); tmpi++) {
//                int tmpidx = childids[tmpi];
//                if (curidx2coef.find(tmpidx) == curidx2coef.end())
//                    bindcoef[tmpi]=0;
//                else
//                    bindcoef[tmpi]=curidx2coef[tmpidx];
//            }
//            bindcoefmap[curparent]=bindcoef;
//        }
//
//        for (auto item: bindcoefmap) {
//            bindNode->addCoefs(item.first, item.second);
//        }
//
//        // decrease refnum for each child
//        for (auto curnode: curchildren) {
//            int curchildid = curnode->getNodeId();
//            curnode->decRefNumFor(curchildid);
//        }
//
//        // add the ref of cur node to the bind node
//        bindNode->setRefNum(tbid, tbnode->getRefNumFor(tbid));  // it seems no use here
//        // clean the childs of cur node and add bind node as child
//        tbnode->cleanChilds();
//        vector<ECNode*> newChildNodes = {bindNode};
//        tbnode->setChilds(newChildNodes);
//        tbnode->addCoefs(tbid, {1});
//    }
//
//    // 3. add tbnode into ecmap
//    _ecNodeMap.insert(make_pair(bindid, bindNode));
//    // 4. deal with cluster
//    int clusterIdx = findCluster(childids);
//    Cluster* curCluster;
//    if (clusterIdx == -1) {
//        curCluster = new Cluster(childids, bindid);
//        _clusterMap.push_back(curCluster);
//    } else {
//        curCluster = _clusterMap[clusterIdx];
//        curCluster->addParent(bindid);
//    }
//    // TODO: set optimization?
//    curCluster->setOpt(0); // BindX has opt level 0;
//    
//    // update ref for childnodes?
//    for (int i=0; i<childnodes.size(); i++) {
//        int curcid = childids[i];
//        int curref = childnodes[i]->getRefNumFor(curcid);
//        childnodes[i]->setRefNum(curcid, curref+1);
//    }
//    curCluster->dump();
//
//    // ECNode* testnode = _ecNodeMap[bindid];
//    // cout << "ECDAG::BindXNotAligned. children of bindnode " << bindid << ": ";
//    // for (auto item: testnode->getChildren()) {
//    //     cout << item->getNodeId() << " ";
//    // }
//    // cout << endl;
//    return bindid;
//}
//
//void ECDAG::BindY(int pidx, int cidx) {
//  cout << "ECDAG::BindY pidx = " << pidx << ", cidx = " << cidx << endl;
//  unordered_map<int, ECNode*>::const_iterator curNode = _ecNodeMap.find(pidx);
//  assert (curNode != _ecNodeMap.end());
//  ECNode* toaddNode = _ecNodeMap[pidx];
//  vector<ECNode*> childNodes = toaddNode->getChildren();
//
//  vector<int> childids;
//  for (int i=0; i<childNodes.size(); i++) childids.push_back(childNodes[i]->getNodeId());  
//
//  cout << "ECDAG::BindY.childids: ";
//  for (auto item: childids) 
//      cout << item << " ";
//  cout << endl;
// 
//  curNode = _ecNodeMap.find(cidx);
//  assert (curNode != _ecNodeMap.end());
//  ECNode* consNode = _ecNodeMap[cidx];
//
//  toaddNode->setConstraint(true, cidx);
//
//  // deal with cluster
//  int clusterid = findCluster(childids);
//  assert (clusterid != -1);
//  Cluster* cluster = _clusterMap[clusterid];
//  // TODO: set optimization?
//  cluster->setOpt(1);
//}
//
//vector<int> ECDAG::toposort() {
//
//  vector<int> toret;
//  
//  // We maintain 3 data-structures for topological sorting
//  unordered_map<int, vector<int>> child2Parent; // given childId, get the parent list of this child
//  unordered_map<int, vector<int>> inNum2List; // given incoming number, get the nodeId List
//  unordered_map<int, int> id2InNum; // given nodeId, figure out current inNum
//
//  for (auto item: _ecNodeMap) {
//    int nodeId = item.first;
//    ECNode* curNode = item.second;
//    vector<ECNode*> childNodes = curNode->getChildren();
//    int inNum = childNodes.size();
//    id2InNum.insert(make_pair(nodeId, inNum));
//
//    // maintain child->parent
//    for (auto item: childNodes) {
//      int childId = item->getNodeId();
//      if (child2Parent.find(childId) == child2Parent.end()) {
//        vector<int> list={nodeId};
//        child2Parent.insert(make_pair(childId, list));
//      } else {
//        child2Parent[childId].push_back(nodeId);
//      }
//    }
//
//    // maintain inNum2List
//    if (inNum2List.find(inNum) == inNum2List.end()) {
//      vector<int> list={nodeId};
//      inNum2List.insert(make_pair(inNum, list));
//    } else {
//      inNum2List[inNum].push_back(nodeId);
//    }
//  }
//
////  for (auto item: child2Parent) {
////    cout << "child: " << item.first << ", parentList: ";
////    for (auto p: item.second) cout << p << " ";
////    cout << endl;
////  }
////
////  for (auto item: inNum2List) {
////    cout << "inNum: " << item.first << ", List:";
////    for (auto nid: item.second) cout << " " << nid;
////    cout << endl;
////  }
//
//  // in each iteration we check nodes with inNum = 0
//  while (true) {
//    if (inNum2List.find(0) == inNum2List.end()) break;
//    if (inNum2List[0].size() == 0) break;
//
//    // take out the list with incoming number equals to 0
//    vector<int> zerolist = inNum2List[0];
////    cout << "zerolist1: ";
////    for (auto xl: zerolist) cout << xl << " ";
////    cout << endl;
//    // add items in zerolist to toret list
//    for (auto id: zerolist) toret.push_back(id);
//    inNum2List[0].clear();
//    for (auto id: zerolist) {
//      // do something to corresponding parent
//      vector<int> idparentlist = child2Parent[id];
////      cout << "parentlist for " << id << ": ";
////      for (auto xl: idparentlist) cout << xl << " ";
////      cout << endl;
//      for (auto p: idparentlist) {
//        // figure out current inNum
//        int curInNum = id2InNum[p];
//        // figure out updated inNum
//        int updatedInNum = curInNum-1;
//        // update id2InNum for p
//        id2InNum[p] = updatedInNum;
////	cout << "inNum for " << p << ": " << curInNum << " -> " << updatedInNum << endl;
//
//        // remove p from curInNum list
//        vector<int>::iterator ppos = find(inNum2List[curInNum].begin(), inNum2List[curInNum].end(), p);
//        inNum2List[curInNum].erase(ppos);
//        // add to updated list
//        if (inNum2List.find(updatedInNum) == inNum2List.end()) {
//          vector<int> updatedlist={p};
//          inNum2List.insert(make_pair(updatedInNum, updatedlist));
//        } else {
//          inNum2List[updatedInNum].push_back(p);
//        }
//      }
//    }
//  }
//  return toret;
//}
//
//ECNode* ECDAG::getNode(int cidx) {
//  assert (_ecNodeMap.find(cidx) != _ecNodeMap.end());
//  return _ecNodeMap[cidx];
//}
//
//vector<int> ECDAG::getHeaders() {
//  return _ecHeaders;
//}
//
//vector<int> ECDAG::getLeaves() {
//  vector<int> toret;
//  for (auto item: _ecNodeMap) {
//    int idx = item.first;
//    ECNode* node = item.second;
//    if (node->getChildNum() == 0) toret.push_back(idx);
//  }
//  sort(toret.begin(), toret.end());
//  return toret;
//}
//
//void ECDAG::reconstruct(int opt) {
//  if (opt == 0) {
//    // enable BindX automatically
//    Opt0();
//  } else if (opt == 1) {
//    Opt1();
//  } else if (opt == 3) {
//    // distributed repair
//    Opt3();
//  }
//}
//
//void ECDAG::optimize(int opt, 
//                     unordered_map<int, pair<string, unsigned int>> objlist,                            
//                     unordered_map<unsigned int, string> ip2Rack,                            
//                     int ecn,
//                     int eck,
//                     int ecw) {
//  if (opt == 0) {
//    // enable BindX automatically to reduce network traffic
//    Opt0();
//  } else if (opt == 1) {
//    Opt1();
//  } else if (opt == 2) {
//    unordered_map<int, string> cid2Rack;
//    for (auto item: _ecNodeMap) {
////      ECNode* curnode = item.second;
////      unsigned int curip = curnode->getIp();
////      string rack = ip2Rack[curip];
////      int cid = item.first;
////      cout << "cid: " << cid << ", ip: " << RedisUtil::ip2Str(curip) << endl;
////      cid2Rack.insert(make_pair(cid, rack));
//    }
//    Opt2(cid2Rack);
//  }
//}
//
//void ECDAG::optimize2(int opt, 
//                     unordered_map<int, unsigned int>& cid2ip,   
//                     unordered_map<unsigned int, string> ip2Rack,                            
//                     int ecn, int eck, int ecw,
//                     unordered_map<int, unsigned int> sid2ip,
//                     vector<unsigned int> allIps,
//                     bool locality) {
//  if (opt == 2) {
//    unordered_map<int, string> cid2Rack;
//    for (auto item: _ecNodeMap) {
//      int cid = item.first;
//      unsigned int curip = cid2ip[cid];
//      string rack = ip2Rack[curip];
//      cid2Rack.insert(make_pair(cid, rack));
//    }
//    Opt2(cid2Rack);
//
//    // after we run Opt2, we need to recheck ip in cid2ip
//    vector<int> toposeq = toposort();
//    for (int i=0; i<toposeq.size(); i++) {
//      int curcid = toposeq[i];
//      ECNode* cnode = getNode(curcid);
//      vector<unsigned int> candidates = cnode->candidateIps(sid2ip, cid2ip, allIps, ecn, eck, ecw, locality);
//      srand((unsigned)time(0));
//      int randomidx = randomidx % candidates.size();
//      unsigned int ip = candidates[randomidx];
//      cid2ip.insert(make_pair(curcid, ip));
//    }
//  }
//}
//
//
//void ECDAG::Opt0() {
//  // check all the clusters and enforce Bind
//  for (auto curCluster: _clusterMap) {
//    if (curCluster->getOpt() == -1) {
//      BindX(curCluster->getParents());
//      curCluster->setOpt(0);
//    }
//  } 
//}
//
//void ECDAG::Opt1() {
//  // add constraint for computation nodes;
//  for (auto curCluster: _clusterMap) {
//    if (curCluster->getOpt() == -1) {
//      vector<int> childs = curCluster->getChilds();
//      vector<int> parents = curCluster->getParents();
//      srand((unsigned)time(0));
//      int randomidx = rand() % childs.size();
//      if (parents.size() == 1) {
//        // addConstraint? 
//        BindY(parents[0], childs[randomidx]);
//      } else if (parents.size() > 1) {
//        int bindnodeid = BindX(parents);
//        BindY(bindnodeid, childs[randomidx]);
//      }
//      curCluster->setOpt(1);
//    }
//  }
//}
//
//void ECDAG::Opt2(unordered_map<int, string> n2Rack) {
//  // 1. now we have all cids corresponding racks, iterate through all clusters
//  vector<int> deletelist;
//  for (int clusteridx = 0; clusteridx < _clusterMap.size(); clusteridx++) {
//    Cluster* curCluster = _clusterMap[clusteridx];
//    if (curCluster->getOpt() != -1) continue;
//    if (ECDAG_DEBUG_ENABLE) cout << "ECDAG::Opt2.deal with ";
//    if (ECDAG_DEBUG_ENABLE) curCluster->dump();
//    vector<int> curChilds = curCluster->getChilds();
//    vector<int> curParents = curCluster->getParents();
//    int numoutput = curParents.size();
//
//    // 1.1 sort the childs into racks
//    unordered_map<string, vector<int>> subchilds;
//    for (int i=0; i<curChilds.size(); i++) {
//      string r = n2Rack[curChilds[i]];
//      if (subchilds.find(r) == subchilds.end()) {
//        vector<int> tmp={curChilds[i]};
//        subchilds.insert(make_pair(r, tmp));
//      } else subchilds[r].push_back(curChilds[i]);
//    }
//    if (ECDAG_DEBUG_ENABLE) cout << "ECDAG::childs are sorted into " << subchilds.size() << " racks" << endl;
//
//    if (numoutput == 1) {
//      // we deploy pipelining technique for current cluster
//      if (ECDAG_DEBUG_ENABLE) cout << "numoutput == 1, deploy pipelining optimization" << endl;
//      int parent = curParents[0];
//      ECNode* parentnode = _ecNodeMap[parent];
//      bool isProot = false;
//      if (find(_ecHeaders.begin(), _ecHeaders.end(), parent) != _ecHeaders.end()) isProot = true;
//      string prack = n2Rack[parent];
//
//      // clean ref for child
//      for (auto curcid: curChilds) {
//        ECNode* curcnode = _ecNodeMap[curcid];
//        curcnode->cleanRefNumFor(curcid);
//      }
//
//      deque<int> dataqueue;
//      deque<int> coefqueue;
//
//      // version 1
//      for (auto group: subchilds) {
//        string crack = group.first;
//        if (crack == prack) continue;
//        for (auto c: group.second) {
//          dataqueue.push_back(c);
//          int curcoef = parentnode->getCoefOfChildForParent(c, parent);
//          coefqueue.push_back(curcoef);
//        }
//      }
//
//      // for version 1 and version 2
//      if (subchilds.find(prack) != subchilds.end()) {
//        for (auto c: subchilds[prack]) {
//          dataqueue.push_back(c);
//          int curcoef = parentnode->getCoefOfChildForParent(c, parent);
//          coefqueue.push_back(curcoef);
//        }
//      }
//      while (dataqueue.size() >= 2) {
//        vector<int> datav;
//        vector<int> coefv;
//        for (int j=0; j<2; j++) {
//          int tmpd(dataqueue.front()); dataqueue.pop_front();
//          int tmpc(coefqueue.front()); coefqueue.pop_front();
//          datav.push_back(tmpd);
//          coefv.push_back(tmpc);
//        }
//        int tmpid;
//        if (dataqueue.size() > 0) tmpid = _optId++;
//        else {
//          tmpid = parent;
//        }
//        Join(tmpid, datav, coefv);
//        BindY(tmpid, datav[1]);
//        dataqueue.push_front(tmpid);
//        coefqueue.push_front(1);
//      }
//      // add curCluster to delete list
//      deletelist.push_back(clusteridx);
//      
//      if (!isProot) {
//        // delete parent from root
//        vector<int>::iterator p = find(_ecHeaders.begin(), _ecHeaders.end(), parent);
//        if (p != _ecHeaders.end()) _ecHeaders.erase(p);
//      }
//    } else {
//  
//      if (subchilds.size() == 1) {
//        BindX(curParents);
//        continue;  // there is no space for current group to optimize
//      }
//  
//      // 1.2 globalChilds and globalCoefs maintains the outer cluster.
//      vector<int> globalChilds;
//      unordered_map<int, vector<int>> globalCoefs;
//  
//      bool update = false;
//      // 1.3 check for each group of subchilds for subcluster
//      for (auto item: subchilds) {
//        vector<int> itemchilds = item.second;
//        vector<int> subparents;
//        
//        if (ECDAG_DEBUG_ENABLE) {
//          cout << "deal with subgroup ( ";
//          for (auto cid: itemchilds) cout << cid <<" ";
//          cout << "), rack: " << item.first << endl;
//        }
//  
//        if (itemchilds.size() > numoutput) {
//          update = true;
//          if (ECDAG_DEBUG_ENABLE) cout << "inputsize = " << itemchilds.size() << ", outputsize = " << numoutput << ", there is space for optimization" << endl;
//          // we will reconstruct this group, clean ref for all itemchilds
//          for (int i=0; i<itemchilds.size(); i++) {
//            ECNode* itemchildnode = _ecNodeMap[itemchilds[i]];
//            itemchildnode->cleanRefNumFor(itemchilds[i]);
//          }
//          // we can create a new subcluster for this group of childs, add subparents
//          for (int i=0; i<numoutput; i++) {
//            int tmpid = _optId++;
//            subparents.push_back(tmpid);
//            globalChilds.push_back(tmpid);        // update globalChilds here
//          }
//  
//          if (ECDAG_DEBUG_ENABLE) {
//            cout << "updated subparents: ( ";
//            for (auto cid: subparents) cout << cid << " ";
//            cout << ")" << endl;
//            cout << "updated globalchilds: ( ";
//            for (auto cid: globalChilds) cout << cid << " ";
//            cout << ")" << endl;
//          }
//  
//          // for each global parent, we need to figure out corresponding coefs to create tmpparent
//          for (int i=0; i<numoutput; i++) {
//            int parent = curParents[i];
//            ECNode* parentnode = _ecNodeMap[parent];
//            int tmpparent = subparents[i];
//  
//            vector<int> tmpcoef;
//            // find corresponding coefs to calculate tmpparent 
//            for (int i=0; i<itemchilds.size(); i++) {
//              int tmpchild = itemchilds[i];
//              int tmpc = parentnode->getCoefOfChildForParent(tmpchild, parent);
//              tmpcoef.push_back(tmpc);
//            }
//            // for each itemchid, ref-=1
//            Join(tmpparent, itemchilds, tmpcoef);
//            if (ECDAG_DEBUG_ENABLE) {
//              cout << tmpparent << " = ( ";
//              for (auto c: tmpcoef) cout << c << " ";
//              cout << ") * ( ";
//              for (auto c: itemchilds) cout << c << " ";
//              cout << ")" << endl;
//              cout << "current cluster.size = " << _clusterMap.size() << endl;
//            }
//
//            // update globalCoefs here
//            if (globalCoefs.find(parent) == globalCoefs.end()) {
//              vector<int> tmp;
//              globalCoefs.insert(make_pair(parent, tmp));
//            }
//            for (int j=0; j<numoutput; j++) {
//              if (j == i) globalCoefs[parent].push_back(1);
//              else globalCoefs[parent].push_back(0);
//            }
//          }
//          int bindid=BindX(subparents);
//          BindY(bindid, itemchilds[0]); // we also need to update in cid2ip
//        } else {
//          // we just pass itemchilds and corresponding coefs for parent
//          for (int i=0; i<itemchilds.size(); i++) {
//            globalChilds.push_back(itemchilds[i]);
//            ECNode* itemchildnode = _ecNodeMap[itemchilds[i]];
//            itemchildnode->cleanRefNumFor(itemchilds[i]);
//          }
//          for (int i=0; i<numoutput; i++) {
//            int parent = curParents[i];
//            ECNode* parentnode = _ecNodeMap[parent];
//            // find corresponding coefs to calculate parent
//            for (int j=0; j<itemchilds.size(); j++) {
//              int tmpchild = itemchilds[j];
//              int tmpc = parentnode->getCoefOfChildForParent(tmpchild, parent);
//              if (globalCoefs.find(parent) == globalCoefs.end()) {
//                vector<int> tmp;
//                globalCoefs.insert(make_pair(parent, tmp));
//              }
//              globalCoefs[parent].push_back(tmpc);
//            }
//          }
//        }
//      } 
//      // 1.4 update globalChilds and globalCoefs for current cluster
////      if (update) {
//        for (int i=0; i<curParents.size(); i++) {
//          int parent = curParents[i];
//          assert (globalCoefs.find(parent) != globalCoefs.end());
//          vector<int> coefs = globalCoefs[parent];
//          Join(parent, globalChilds, coefs);
//        }
//        BindX(curParents);
//        deletelist.push_back(clusteridx);
////      } else {
////        BindX(curParents);
////      }
//  
//      // check whether global Childs are in roots
//      for (auto c: globalChilds) {
//        vector<int>::iterator inroot = find(_ecHeaders.begin(), _ecHeaders.end(), c);
//        if (inroot != _ecHeaders.end()) _ecHeaders.erase(inroot);
//      }
//    }
//  }
//  // delete cluster in deletelist
//  vector<Cluster*>::iterator it;;
//  sort(deletelist.begin(), deletelist.end());
//  for (int i=deletelist.size()-1; i >= 0; i--) {
//    it = _clusterMap.begin();
//    int idx = deletelist[i];
//    it += idx;
//    _clusterMap.erase(it);
//  }
//}
//
//void ECDAG::Opt3() {
//    //cout << "ECDAG::Opt3" << endl;
//    for (auto cluster: _clusterMap)  {
//        vector<int> parents = cluster->getParents();
//        vector<int> childs = cluster->getChilds() ;
//        for (auto pid: parents) {
//            assert(_ecNodeMap.find(pid) != _ecNodeMap.end());
//            ECNode* pnode = _ecNodeMap[pid];
//
//            // child nodes
//            vector<ECNode*> cnodes = pnode->getChildren();
//            vector<int> cidxs;
//            for (auto cn: cnodes) {
//                cidxs.push_back(cn->getNodeId());
//            }
//
//            // coef
//            unordered_map<int, vector<int>> coefMap = pnode->getCoefmap();
//            if (coefMap.size() <= 1) 
//                continue;
//
//            // decrease reference num for each children
//            for (auto cn: cnodes) {
//                cn->decRefNumFor(cn->getNodeId());
//            }
//
//            // split
//            for (auto item: coefMap) {
//                // get the idx for the node to compute
//                int computeidx = item.first;
//                // get the coef
//                vector<int> curcoefs = item.second;
//
//                // figure out the shoren child nodes and coef
//                vector<int> shortenidx;
//                vector<ECNode*> shortennodes;
//                vector<int> shortencoef;
//
//                for (int i=0; i<curcoefs.size(); i++) {
//                    if (curcoefs[i] > 0) {
//                        shortenidx.push_back(cidxs[i]);
//                        shortennodes.push_back(_ecNodeMap[cidxs[i]]);
//                        shortencoef.push_back(curcoefs[i]);
//                    }
//                }
//
//                // update shorten child nodes and coef in the computenode
//                ECNode* computenode = _ecNodeMap[computeidx];
//                computenode->setChilds(shortennodes);
//                computenode->addCoefs(computeidx, shortencoef);
//
//                // update ref num for each shorten child
//                for (auto cn: shortennodes)
//                    cn->incRefNumFor(cn->getNodeId());
//
//                // update child cluster
//                int clusterid = findCluster(shortenidx);
//                if (clusterid != -1) {
//                    _clusterMap[clusterid]->addParent(computeidx);
//                    _clusterMap[clusterid]->setOpt(3);
//                } else {
//                    Cluster* cluster = new Cluster(shortenidx, computeidx);
//                    _clusterMap.push_back(cluster);
//                }
//            }
//
//            // update parent cluster
//            int clusterid = findCluster(cidxs);
//            assert(clusterid != -1);
//            _clusterMap[clusterid]->removeParent(pid);
//
//            // remove parent node
//            _ecNodeMap.erase(_ecNodeMap.find(pid));
//        }
//    }
//
//    //for (auto cluster: _clusterMap) {
//    //    cluster->dump();
//    //}
//}
//
//unordered_map<int, AGCommand*> ECDAG::parseForOEC(unordered_map<int, unsigned int> cid2ip,
//                                      string stripename, 
//                                      int n, int k, int w, int num,
//                                      unordered_map<int, pair<string, unsigned int>> objlist) {
//
//  // adjust refnum for heads
//  for (int i=0; i<_ecHeaders.size(); i++) {
//    int nid = _ecHeaders[i];
//    _ecNodeMap[nid]->incRefNumFor(nid);
//  }
//
//  vector<AGCommand*> toret;
//  vector<int> sortedList = toposort();
//
//  // we first put commands into a map
//  unordered_map<int, AGCommand*> agCmds;
//  for (int i=0; i<sortedList.size(); i++) {
//    int cidx = sortedList[i];
//    ECNode* node = getNode(cidx);
//    unsigned int ip = cid2ip[cidx];
//    node->parseForOEC(ip);
//    AGCommand* cmd = node->parseAGCommand(stripename, n, k, w, num, objlist, cid2ip);
//    if (cmd) agCmds.insert(make_pair(cidx, cmd));
//  }
//
//  // we can merge the following commands
//  // 2. load & cache
//  // 3. fetch & compute & cache
//  // when ip of these two commands are the same
//  if (w == 1) {
//    unordered_map<int, int> tomerge;
//    for (auto item: agCmds) {
//      int cid = item.first;
//      int sid = cid/w;
//      AGCommand* cmd = agCmds[cid];
//      if (cmd->getType() != 3) continue;
//      // now we start with a type 3 command
//      unsigned int ip = cmd->getSendIp(); 
//      // check child
//      int childid;
//      AGCommand* childCmd;
//      bool found = false;
//      vector<int> prevCids = cmd->getPrevCids();
//      vector<unsigned int> prevLocs = cmd->getPrevLocs();
//      for (int i=0; i<prevCids.size(); i++) {
//        int childCid = prevCids[i];
//        unsigned int childip = prevLocs[i];
//        if (agCmds.find(childCid) == agCmds.end()) continue;
//        AGCommand* cCmd = agCmds[childCid];
//        if (cCmd->getType() != 2) continue;
//        if (childip == ip) {
//          childid = childCid;
//          childCmd = cCmd;
//          found = true;
//          break;
//        }
//      }
//      if (!found) continue;
//      // we find a child that shares the same location with the parent
//      tomerge.insert(make_pair(cid, childid));
//    }
//    // now in tomerge, item.first and item.second are sent to the same node, 
//    // we can merge them into a single command to avoid local redis I/O
//    for (auto item: tomerge) {
//      int cid = item.first;
//      int childid = item.second;
//      if (ECDAG_DEBUG_ENABLE) cout << "ECDAG::parseForOEC.merge " << cid << " and " << childid << endl;
//      AGCommand* cmd = agCmds[cid];
//      AGCommand* childCmd = agCmds[childid];
//      // get information from existing commands
//      unsigned int ip = cmd->getSendIp();
//      string readObjName = childCmd->getReadObjName();
//      vector<int> readCidList = childCmd->getReadCidList();  // actually, when w=1, there is only one symbol in readCidList
//      unordered_map<int, int> readCidListRef = childCmd->getCacheRefs();
////      assert (readCidList.size() == 1);
//      int nprev = cmd->getNprevs();
//      vector<int> prevCids = cmd->getPrevCids();
//      vector<unsigned int> prevLocs = cmd->getPrevLocs();
//      unordered_map<int, int> computeCidRef = cmd->getCacheRefs();
//      unordered_map<int, vector<int>> computeCoefs = cmd->getCoefs();
//      // update prevLocs
//      vector<int> reduceList;
//      for (int i=0; i<prevCids.size(); i++) {
//        if (find(readCidList.begin(), readCidList.end(), prevCids[i]) != readCidList.end()) {
//          reduceList.push_back(prevCids[i]);
//          prevLocs[i] = 0; // this means we can get in the same process
//        }
//      }
//      // update refs
//      // considering that if ref in readCidList is larger than 1, we also need to cache it in redis
//      for (auto item: computeCoefs) {
//        for (auto tmpc: reduceList) {
//          if (readCidListRef.find(tmpc) != readCidListRef.end()) {
//            readCidListRef[tmpc]--;
//            if (readCidListRef[tmpc] == 0) {
//              readCidListRef.erase(tmpc);
//            }
//          }
//        }
//      }
//      // now we can merge childCmd and curCmd into a new command
//      unordered_map<int, int> mergeref;
//      for (auto item: readCidListRef) mergeref.insert(item);
//      for (auto item: computeCidRef) mergeref.insert(item);
//      AGCommand* mergeCmd = new AGCommand();
//      mergeCmd->buildType7(7, ip, stripename, w, num, readObjName, readCidList, nprev, prevCids, prevLocs, computeCoefs, mergeref);
//      // remove cid and childid commands in agCmds and add this command
//      agCmds.erase(cid);
//      agCmds.erase(childid);
//      agCmds.insert(make_pair(cid, mergeCmd));
//    }
//  } 
//
//  //for (auto item: agCmds) item.second->dump();
//
//  return agCmds;
//}
//
//unordered_map<int, AGCommand*> ECDAG::parseForOECWithVirtualLeaves(unordered_map<int, unsigned int> cid2ip,
//                                      unordered_map<int, int> virleaves,
//                                      string stripename, 
//                                      int n, int k, int w, int num,
//                                      unordered_map<int, pair<string, unsigned int>> objlist) {
//
//  // adjust refnum for heads
//  for (int i=0; i<_ecHeaders.size(); i++) {
//    int nid = _ecHeaders[i];
//    _ecNodeMap[nid]->incRefNumFor(nid);
//  }
//
//  vector<AGCommand*> toret;
//  vector<int> sortedList = toposort();
//
//  // we first put commands into a map
//  unordered_map<int, AGCommand*> agCmds;
//  for (int i=0; i<sortedList.size(); i++) {
//    int cidx = sortedList[i];
//    ECNode* node = getNode(cidx);
//    unsigned int ip = cid2ip[cidx];
//    //node->parseForOEC(ip);
//    node->parseForOECWithVirtualLeaves(ip,virleaves,w);
//    AGCommand* cmd = node->parseAGCommand(stripename, n, k, w, num, objlist, cid2ip);
//    if (cmd) agCmds.insert(make_pair(cidx, cmd));
//  }
//
//  // we can merge the following commands
//  // 2. load & cache
//  // 3. fetch & compute & cache
//  // when ip of these two commands are the same
//  if (w == 1) {
//    unordered_map<int, int> tomerge;
//    for (auto item: agCmds) {
//      int cid = item.first;
//      int sid = cid/w;
//      AGCommand* cmd = agCmds[cid];
//      if (cmd->getType() != 3) continue;
//      // now we start with a type 3 command
//      unsigned int ip = cmd->getSendIp(); 
//      // check child
//      int childid;
//      AGCommand* childCmd;
//      bool found = false;
//      vector<int> prevCids = cmd->getPrevCids();
//      vector<unsigned int> prevLocs = cmd->getPrevLocs();
//      for (int i=0; i<prevCids.size(); i++) {
//        int childCid = prevCids[i];
//        unsigned int childip = prevLocs[i];
//        if (agCmds.find(childCid) == agCmds.end()) continue;
//        AGCommand* cCmd = agCmds[childCid];
//        if (cCmd->getType() != 2) continue;
//        if (childip == ip) {
//          childid = childCid;
//          childCmd = cCmd;
//          found = true;
//          break;
//        }
//      }
//      if (!found) continue;
//      // we find a child that shares the same location with the parent
//      tomerge.insert(make_pair(cid, childid));
//    }
//    // now in tomerge, item.first and item.second are sent to the same node, 
//    // we can merge them into a single command to avoid local redis I/O
//    for (auto item: tomerge) {
//      int cid = item.first;
//      int childid = item.second;
//      if (ECDAG_DEBUG_ENABLE) cout << "ECDAG::parseForOEC.merge " << cid << " and " << childid << endl;
//      AGCommand* cmd = agCmds[cid];
//      AGCommand* childCmd = agCmds[childid];
//      // get information from existing commands
//      unsigned int ip = cmd->getSendIp();
//      string readObjName = childCmd->getReadObjName();
//      vector<int> readCidList = childCmd->getReadCidList();  // actually, when w=1, there is only one symbol in readCidList
//      unordered_map<int, int> readCidListRef = childCmd->getCacheRefs();
////      assert (readCidList.size() == 1);
//      int nprev = cmd->getNprevs();
//      vector<int> prevCids = cmd->getPrevCids();
//      vector<unsigned int> prevLocs = cmd->getPrevLocs();
//      unordered_map<int, int> computeCidRef = cmd->getCacheRefs();
//      unordered_map<int, vector<int>> computeCoefs = cmd->getCoefs();
//      // update prevLocs
//      vector<int> reduceList;
//      for (int i=0; i<prevCids.size(); i++) {
//        if (find(readCidList.begin(), readCidList.end(), prevCids[i]) != readCidList.end()) {
//          reduceList.push_back(prevCids[i]);
//          prevLocs[i] = 0; // this means we can get in the same process
//        }
//      }
//      // update refs
//      // considering that if ref in readCidList is larger than 1, we also need to cache it in redis
//      for (auto item: computeCoefs) {
//        for (auto tmpc: reduceList) {
//          if (readCidListRef.find(tmpc) != readCidListRef.end()) {
//            readCidListRef[tmpc]--;
//            if (readCidListRef[tmpc] == 0) {
//              readCidListRef.erase(tmpc);
//            }
//          }
//        }
//      }
//      // now we can merge childCmd and curCmd into a new command
//      unordered_map<int, int> mergeref;
//      for (auto item: readCidListRef) mergeref.insert(item);
//      for (auto item: computeCidRef) mergeref.insert(item);
//      AGCommand* mergeCmd = new AGCommand();
//      mergeCmd->buildType7(7, ip, stripename, w, num, readObjName, readCidList, nprev, prevCids, prevLocs, computeCoefs, mergeref);
//      // remove cid and childid commands in agCmds and add this command
//      agCmds.erase(cid);
//      agCmds.erase(childid);
//      agCmds.insert(make_pair(cid, mergeCmd));
//    }
//  } 
//
//  //for (auto item: agCmds) item.second->dump();
//
//  return agCmds;
//}
//
//vector<AGCommand*> ECDAG::persist(unordered_map<int, unsigned int> cid2ip, 
//                                  string stripename,
//                                  int n, int k, int w, int num,
//                                  unordered_map<int, pair<string, unsigned int>> objlist) {
//  vector<AGCommand*> toret;
//  // sort headers
//  sort(_ecHeaders.begin(), _ecHeaders.end());
//  int numblks = _ecHeaders.size()/w;
//  if (ECDAG_DEBUG_ENABLE) cout << "ECDAG:: persist. numblks: " << numblks << endl;
//  for (int i=0; i<numblks; i++) {
//    int cid = _ecHeaders[i*w];
//    int sid = cid/w;
//    string objname = objlist[sid].first;
//    unsigned int ip = objlist[sid].second;
//    vector<int> prevCids;
//    vector<unsigned int> prevLocs;
//    for (int j=0; j<w; j++) {
//      int cid = sid*w+j;
//      ECNode* cnode = getNode(cid);
//      unsigned int cip = cnode->getIp();
//      prevCids.push_back(cid);
//      prevLocs.push_back(cip);
//    }
//    AGCommand* cmd = new AGCommand(); 
//    cmd->buildType5(5, ip, stripename, w, num, w, prevCids, prevLocs, objname);
//    cmd->dump();
//    toret.push_back(cmd);
//  }
//  return toret;
//}
//
//vector<AGCommand*> ECDAG::persistWithInfo(unordered_map<int, unsigned int> cid2ip, 
//        string stripename,
//        int n, int k, int w, int num,
//        unordered_map<int, pair<string, unsigned int>> objlist,
//        vector<int> plist) {
//    vector<AGCommand*> toret;
//    for (int i=0; i<plist.size(); i++) {
//        int sid = plist[i];
//        string objname = objlist[sid].first;
//        unsigned int ip = objlist[sid].second;
//        vector<int> prevCids;
//        vector<unsigned int> prevLocs;
//        for (int j=0; j<w; j++) {
//            int cid = sid*w+j;
//            ECNode* cnode = getNode(cid);
//            unsigned int cip = cnode->getIp();
//            prevCids.push_back(cid);
//            prevLocs.push_back(cip);
//        }
//        AGCommand* cmd = new AGCommand(); 
//        cmd->buildType5(5, ip, stripename, w, num, w, prevCids, prevLocs, objname);
//        //cmd->dump();
//        toret.push_back(cmd);
//    }
//    return toret;    
//}
//
//vector<AGCommand*> ECDAG::persistLayerWithInfo(unordered_map<int, unsigned int> cid2ip, 
//        string stripename,
//        int n, int k, int w, int num,
//        unordered_map<int, pair<string, unsigned int>> objlist,
//        vector<int> plist, string layer) {
//    vector<AGCommand*> toret;
//    for (int i=0; i<plist.size(); i++) {
//        int sid = plist[i];
//        string objname = objlist[sid].first;
//        unsigned int ip = objlist[sid].second;
//        vector<int> prevCids;
//        vector<unsigned int> prevLocs;
//        for (int j=0; j<w; j++) {
//            int cid = sid*w+j;
//            ECNode* cnode = getNode(cid);
//            unsigned int cip = cnode->getIp();
//            prevCids.push_back(cid);
//            prevLocs.push_back(cip);
//        }
//        AGCommand* cmd = new AGCommand(); 
//        cmd->buildType24(24, ip, stripename, w, num, w, prevCids, prevLocs, objname, layer);
//        //cmd->dump();
//        toret.push_back(cmd);
//    }
//    return toret;    
//}
//
void ECDAG::dump() {
  for (auto id : _ecHeaders) {
    _ecNodeMap[id] ->dump(-1);
    cout << endl;
  }
//  for (auto cluster: _clusterMap) {
//    cluster->dump();
//  }
}
