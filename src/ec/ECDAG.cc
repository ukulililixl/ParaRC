#include "ECDAG.hh"

ECDAG::ECDAG() {
}

ECDAG::~ECDAG() {
    for (auto item: _ecClusterMap)
        delete item.second;
    for (auto item: _ecUnitMap)
        delete item.second;
    for (auto it: _ecNodeMap) {
        if (it.second) {
            delete it.second;
            it.second = nullptr;
        }
    }
    _ecNodeMap.clear();
  
}


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

  //// debug
  //for (int i=0; i<_ecUnitList.size(); i++)  {
  //  cout << _ecUnitMap[_ecUnitList[i]]->dump() << endl;
  //}
}

void ECDAG::clearECCluster() {
  for (auto item: _ecClusterMap)
    delete item.second;
  _clusterId = CSTART;
  _ecClusterMap.clear();
  _ecClusterList.clear();
}

void ECDAG::genECCluster(unordered_map<int, int> coloring, int clustersize) {
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

    // cout << "current layer of ECUnit: ";
    // for (int ui=0; ui<curunitlist.size(); ui++) {
    //   cout << curunitlist[ui] << " ";
    // }
    // cout << endl;

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

    // cout << "current color2unitlist: " << endl;
    for (auto item: color2unitlist) {
        //cout << "  color: " << item.first << ", unit num:  " << item.second.size() << endl;
        unordered_map<string, vector<int>> cstr2unitlist;
        vector<string> cstrlist;
        for (auto tmpunit: item.second) {
            string cstr = _ecUnitMap[tmpunit]->getChildStr();
            if (cstr2unitlist.find(cstr) == cstr2unitlist.end()) {
                vector<int> list = {tmpunit};
                cstr2unitlist.insert(make_pair(cstr, list));
                cstrlist.push_back(cstr);
            } else {
                cstr2unitlist[cstr].push_back(tmpunit);
            }
        }
        //for (auto tmpitem: cstr2unitlist) {
        //    cout << "    cstr: " << tmpitem.first << ", unitlist: ";
        //    for (auto tmpunit: tmpitem.second)
        //        cout << tmpunit << " ";
        //    cout << endl;
        //}

        // we generate ecclusters according to cstr2unitlist; 
        int cstridx=0;
        vector<int> curclusterunits;
        for (int cstridx=0; cstridx<cstrlist.size();) {
            string cstr = cstrlist[cstridx];
            vector<int> curunitlist = cstr2unitlist[cstr];
            if (curclusterunits.size() + curunitlist.size() <= clustersize) {
                // there is still space in the current cluster
                for (auto tmpu: curunitlist)
                    curclusterunits.push_back(tmpu);
                cstridx++;
            } else {
                // current cluster is full
                ECCluster* eccluster = new ECCluster(_clusterId++, curclusterunits);
                _ecClusterMap.insert(make_pair(eccluster->getClusterId(), eccluster));
                _ecClusterList.push_back(eccluster->getClusterId());
                curclusterunits.clear();
                //cout << "    "  << eccluster->dump() << endl;
            }
        }

        if (curclusterunits.size() > 0) {
            ECCluster* eccluster = new ECCluster(_clusterId++, curclusterunits);
            _ecClusterMap.insert(make_pair(eccluster->getClusterId(), eccluster));
            _ecClusterList.push_back(eccluster->getClusterId());
            curclusterunits.clear();
            //cout << "    "  << eccluster->dump() << endl;
        }
    }

    //// for each color, merge corresponding unit
    //for (auto item: color2unitlist) {
    //  vector<int> curunitlist = item.second;
    //  cout << "  color: " << item.first << ", unitnum: " << curunitlist.size() << endl;
    //  ECCluster* eccluster = new ECCluster(_clusterId++, curunitlist);
    //  _ecClusterMap.insert(make_pair(eccluster->getClusterId(), eccluster));
    //  _ecClusterList.push_back(eccluster->getClusterId());
    //}
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
  unordered_map<int, int> opmap;
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

      // collect ops
      if (opmap.find(pcolor) == opmap.end()) {
          opmap.insert(make_pair(pcolor, tmpchilds.size()));
      } else {
          opmap[pcolor]+=tmpchilds.size();
      }
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

  //cout << "opmap: " << endl;
  //for (auto item: opmap) {
  //    cout << "  " << item.first << ": " << item.second << endl;
  //}
}

void ECDAG::genECTasksByECClusters(vector<ECTask*>& tasklist,                                                                                                                         
        int ecn, int eck, int ecw, int blkbytes, int pktbytes, 
        string stripename, vector<string> blocklist,
        unordered_map<int, unsigned int> coloring_res) {
    // cout << "ECUnits: " << endl;
    // for (int i=0; i<_ecUnitList.size(); i++) {
    //     int unitId = _ecUnitList[i];
    //     ECUnit* cunit = _ecUnitMap[unitId];
    //     cout << "  " << cunit->dump() << endl;
    // }

    // for each vertex, figure out the number of referenced ECClusters for it
    unordered_map<int, int> cidRefs;

    // record the vertices generated by shortening
    unordered_map<int, int> shortening;
    unordered_map<int, int> realleaves;
    for (auto cid: _ecLeaves) {
        int bidx = cid/ecw;
        if (bidx < ecn)
            realleaves.insert(make_pair(cid, 0));
        else
            shortening.insert(make_pair(cid, 0));
    }

    //cout << "ECCluster:" << endl;
    for (int i=0; i<_ecClusterList.size(); i++) {
        int clusterId = _ecClusterList[i];
        ECCluster* ccluster = _ecClusterMap[clusterId];
        //cout << "  " << ccluster->dump() << endl;

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
            //cout << "    unit: " << cunit->dump() << endl;

            vector<int> unitchilds = cunit->getChilds();
            for (auto cid: unitchilds) {
                if (childmap.find(cid) == childmap.end()) {
                    childmap.insert(make_pair(cid, true));
                    childlist.push_back(cid);
                }
            }
        }
        sort(childlist.begin(), childlist.end());

        // increase child reference by 1 for the current cluster
        for (auto cid: childlist) {
            if (shortening.find(cid) != shortening.end()) 
                continue;

            if (cidRefs.find(cid) == cidRefs.end()) 
                cidRefs.insert(make_pair(cid, 1));
            else
                cidRefs[cid]++;
        }

        //// update ref in leaveRefs
        //for (auto cid: childlist) {
        //    if (leaveRefs.find(cid)!=leaveRefs.end())
        //        leaveRefs[cid]++;
        //}
        
        //cout << "    childlist: ";
        //for (int tmpi=0; tmpi < childlist.size(); tmpi++) {
        //    cout << childlist[tmpi] << " ";
        //}
        //cout << endl;

        // update coefs for childlist
        for (int j=0; j<unitlist.size(); j++) {
            int unitId = unitlist[j];
            ECUnit* cunit = _ecUnitMap[unitId];
            //cout << "    unit: " << cunit->dump() << endl;
            vector<int> unitchilds = cunit->getChilds();
            vector<int> unitcoefs = cunit->getCoefs();
            int parent = cunit->getParent();

            //cout << "    parent: " << parent << ", childs: ";
            //for (int tmpi=0; tmpi<unitchilds.size(); tmpi++)
            //    cout << unitchilds[tmpi] << " ";
            //cout << ", coefs: ";
            //for (int tmpi=0; tmpi<unitcoefs.size(); tmpi++)
            //    cout << unitcoefs[tmpi] << " ";
            //cout << endl;

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
            
            // cout << "    parent: " << parent << ", newchilds: ";
            // for (int tmpi=0; tmpi<childlist.size(); tmpi++)
            //     cout << childlist[tmpi] << " ";
            // cout << ", newcoefs: ";
            // for (int tmpi=0; tmpi<coeflist.size(); tmpi++)
            //     cout << coeflist[tmpi] << " ";
            // cout << endl;

            parentlist.push_back(parent);
            coefmap.insert(make_pair(parent, coeflist));
        }

        // setup data structures in cluster
        ccluster->setChildList(childlist);
        ccluster->setParentList(parentlist);
        ccluster->setCoefMap(coefmap);
    }

    // check realleaves, merge cid of the same block
    unordered_map<int, vector<int>> bid2cids;
    for (auto item: realleaves) {
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
            //int r = leaveRefs[cid];
            int r = cidRefs[cid];
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
        //cout << "cluster: " << clusterId << endl;
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
            //cout << "  Concact!" << endl;

            int type = 2;
            int bidx = childlist[0] / ecw;
            string blockname = blocklist[bidx];

            ECTask* concactTask = new ECTask();
            concactTask->buildConcatenate(type, loc, childlist, prevlocs, stripename, blockname, ecw, blkbytes, pktbytes);
            tasklist.push_back(concactTask);

        } else {
            //cout << "  FetchCompute!" << endl;
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

            unordered_map<int, int> cur_cid2refs;
            for (auto curcid: parentlist) {
                int r = cidRefs[curcid];
                cur_cid2refs.insert(make_pair(curcid, r));
            }

            ECTask* fetchComputeTask = new ECTask();
            fetchComputeTask->buildFetchCompute(type, loc, childlist, prevlocs, 
                    computelist, stripename, cur_cid2refs, ecw, blkbytes, pktbytes);
            tasklist.push_back(fetchComputeTask);
        }
    }
}

void ECDAG::genDistECTasks(vector<ECTask*>& tasklist,                                                                                                                         
        int ecn, int eck, int ecw, int blkbytes, int pktbytes, 
        string stripename, vector<string> blocklist,
        unordered_map<int, unsigned int> coloring_res) {
    // cout << "ECUnits: " << endl;
    // for (int i=0; i<_ecUnitList.size(); i++) {
    //     int unitId = _ecUnitList[i];
    //     ECUnit* cunit = _ecUnitMap[unitId];
    //     cout << "  " << cunit->dump() << endl;
    // }

    // for each vertex, figure out the number of referenced ECClusters for it
    unordered_map<int, int> cidRefs;

    // record the vertices generated by shortening
    unordered_map<int, int> shortening;
    unordered_map<int, int> realleaves;
    for (auto cid: _ecLeaves) {
        int bidx = cid/ecw;
        if (bidx < ecn)
            realleaves.insert(make_pair(cid, 0));
        else
            shortening.insert(make_pair(cid, 0));
    }

    // figure out cidRefs
    //cout << "ECClusters: " << endl;
    for (int i=0; i<_ecClusterList.size(); i++) {
        int clusterId = _ecClusterList[i];
        ECCluster* ccluster = _ecClusterMap[clusterId];
        //cout << "  " << ccluster->dump() << endl;

        vector<int> unitlist = ccluster->getUnitList();
        vector<int> childlist;
        unordered_map<int, int> childmap;
        vector<int> parentlist;
        // figure out all the childs for the current cluster
        for (int j=0; j<unitlist.size(); j++) {
            int unitId = unitlist[j];
            ECUnit* cunit = _ecUnitMap[unitId];

            vector<int> unitchilds = cunit->getChilds();
            for (auto cid: unitchilds) {
                if (childmap.find(cid) == childmap.end()) {
                    childmap.insert(make_pair(cid, 1));
                    childlist.push_back(cid);
                }
            }
            int unitparent = cunit->getParent();
            parentlist.push_back(unitparent);
        }
        sort(childlist.begin(), childlist.end());

        // increase child reference by 1 for the current cluster
        for (auto cid: childlist) {
            if (shortening.find(cid) != shortening.end()) 
                continue;

            if (cidRefs.find(cid) == cidRefs.end()) 
                cidRefs.insert(make_pair(cid, 1));
            else
                cidRefs[cid]++;
        }

        ccluster->setChildList(childlist);
        ccluster->setParentList(parentlist);
    }

    //cout << "cidRefs:" << endl;
    //for (auto item: cidRefs) {
    //    cout << item.first << ": " << item.second << endl;
    //}

    // check realleaves, merge cid of the same block
    unordered_map<int, vector<int>> bid2cids;
    for (auto item: realleaves) {
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
            //int r = leaveRefs[cid];
            int r = cidRefs[cid];
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
        //cout << "cluster: " << clusterId << endl; 

        vector<int> childlist = ccluster->getChildList();
        vector<int> parentlist = ccluster->getParentList();
        vector<int> unitlist = ccluster->getUnitList();

        // cout << "unitlist: ";
        // for (auto uid: unitlist)
        //     cout << uid << " ";
        // cout << endl;

        // cout << "childlist: ";
        // for (auto cid: childlist)
        //     cout << cid << " ";
        // cout << endl;

        // cout << "parentlist: ";
        // for (auto pid: parentlist) 
        //     cout << pid << " ";
        // cout << endl;

        unsigned int loc = coloring_res[parentlist[0]];

        unordered_map<unsigned int, vector<int>> ip2childlist;
        for (int tmpi=0; tmpi<childlist.size(); tmpi++) {
            int cid = childlist[tmpi];
            unsigned int tmpl = coloring_res[cid];
            if (ip2childlist.find(tmpl) == ip2childlist.end()) {
                vector<int> curlist = {cid};
                ip2childlist.insert(make_pair(tmpl, curlist));
            } else {
                ip2childlist[tmpl].push_back(cid);
            }
        }

        if (ccluster->isConcact(REQUESTOR)) {
            int type = 4;
            int bidx = childlist[0]/ecw;
            string blockname = blocklist[bidx];

            ECTask* concactTask = new ECTask();
            concactTask->buildConcatenate2(type, loc, ip2childlist, stripename, blockname, ecw, blkbytes, pktbytes);
            tasklist.push_back(concactTask);
        } else {
            
            int type = 3;

            vector<ComputeTask*> ctlist;
            for (int tmpi=0; tmpi<unitlist.size(); tmpi++) {
                int unitIdx = unitlist[tmpi];
                ECUnit* cunit = _ecUnitMap[unitIdx];

                vector<int> srclist = cunit->getChilds();
                int parent = cunit->getParent();
                if (parent == REQUESTOR)
                    continue;
                vector<int> coef = cunit->getCoefs();

                vector<int> dstlist = {parent};
                vector<vector<int>> coeflist;
                coeflist.push_back(coef);

                ComputeTask* ct = new ComputeTask(srclist, dstlist, coeflist);
                ctlist.push_back(ct);
            }

            unordered_map<int, int> cid2refs;
            for (int tmpi=0; tmpi<parentlist.size(); tmpi++) {
                int curparent = parentlist[tmpi];
                int ref = cidRefs[curparent];
                cid2refs.insert(make_pair(curparent, ref));
            }

            // create fetch and compute
            ECTask* fetchComputeTask = new ECTask();
            fetchComputeTask->buildFetchCompute2(type, loc, ip2childlist, ctlist,
                    stripename, cid2refs, ecw, blkbytes, pktbytes);
            tasklist.push_back(fetchComputeTask);
        }
    }

}

void ECDAG::genComputeTaskByECUnits(vector<ComputeTask*>& tasklist) {
    for (int i=0; i<_ecUnitList.size(); i++) {
        int unitIdx = _ecUnitList[i];
        ECUnit* cunit = _ecUnitMap[unitIdx];

        vector<int> srclist = cunit->getChilds();
        int parent = cunit->getParent();
        if (parent == REQUESTOR)
            continue;
        vector<int> coef = cunit->getCoefs();

        vector<int> dstlist = {parent};
        vector<vector<int>> coeflist;
        coeflist.push_back(coef);

        ComputeTask* ct = new ComputeTask(srclist, dstlist, coeflist);
        tasklist.push_back(ct);
    }
}

void ECDAG::genConvECTasks(vector<ECTask*>& tasklist,
        int ecn, int eck, int ecw, int blkbytes, int pktbytes,
        string stripename, vector<string> blocklist,
        vector<unsigned int> loclist, int repairIdx) {
    cout << "ECDAG::genConvECTasks" << endl;
    
    // 0. figure out real leaves
    unordered_map<int, vector<int>> bid2cids;
    vector<int> shorteninglist;
    for (int i=0; i<_ecLeaves.size(); i++) {
        int cid = _ecLeaves[i];
        int bid = cid/ecw;
        if (bid < ecn) {
            // a real leaf vertex
            if (bid2cids.find(bid) == bid2cids.end()) {
                vector<int> list = {cid};
                bid2cids.insert(make_pair(bid, list));
            } else {
                bid2cids[bid].push_back(cid);
            }
        } else {
            shorteninglist.push_back(cid);
        }
    }

    // 1. figure out units for repair by transfer 
    unordered_map<unsigned int, vector<int>> ip2transferunits; // map ip to unitIdx for repair by transfer
    unordered_map<int, unsigned int> cid2ip; // map cid generated by repair by transfer to ip

    vector<int> finalunits; // record units for repair node
    unordered_map<unsigned int, vector<int>> ip2cidlist; // map ip to cid for disk read without repair by transfer

    //unordered_map<int, int> cid2refs;

    for (int i=0; i<_ecUnitList.size(); i++) {
        int unitIdx = _ecUnitList[i];
        ECUnit* cunit = _ecUnitMap[unitIdx];

        vector<int> srclist = cunit->getChilds();

        //// cid2refs
        //for (auto cid: srclist) {
        //    if (cid2refs.find(cid) == cid2refs.end())
        //        cid2refs.insert(make_pair(cid, 1));
        //    else
        //        cid2refs[cid]++;
        //}

        int parent = cunit->getParent();
        if (parent == REQUESTOR)
            continue;
        // check whether all the childs are leaf and in the same block

        bool transfer = true;
        int blkidx = -1;
        for (auto cidx: srclist) {
            if (find(_ecLeaves.begin(), _ecLeaves.end(), cidx) == _ecLeaves.end()) {
                transfer = false;
                break;
            }

            // cidx is a leaf
            // figure out corresponding blockidx
            int tmpblkidx = cidx / ecw;

            if (tmpblkidx >= ecn) {
                // This is a shortening index
                transfer = false;
                break;
            }

            if (blkidx == -1)
                blkidx = tmpblkidx;

            if (tmpblkidx != blkidx) {
                transfer = false;
                break;
            }
        }

        if (transfer) {
            // the current unit is for repair by transfer
            unsigned int ip = loclist[blkidx];
            if (ip2transferunits.find(ip) == ip2transferunits.end()) {
                vector<int> list = {unitIdx};
                ip2transferunits.insert(make_pair(ip, list));
            } else {
                ip2transferunits[ip].push_back(unitIdx);
            }
        } else {
            finalunits.push_back(unitIdx);
        }
    }

    //cout << "ECDAG::genConvECTasks.ip2transferunits: " << endl;
    //for (auto item: ip2transferunits) {
    //    unsigned int ip = item.first;
    //    vector<int> list = item.second;
    //    cout << "  ip: " << RedisUtil::ip2Str(ip) << ", ";
    //    for (auto id: list)
    //        cout << id << " ";
    //    cout << endl;
    //}

    //cout << "ECDAG::genConvECTasks.finalunits: ";
    //for (auto item: finalunits)
    //    cout << item << " ";
    //cout << endl;

    // 2. generate readbid2cids for finalunits
    unordered_map<int, int> finalrefs;
    unordered_map<int, vector<int>> readbid2cids;
    for (auto unitidx: finalunits) {
        ECUnit* cunit = _ecUnitMap[unitidx];
        vector<int> srclist = cunit->getChilds();
        for (auto cid: srclist) {
            if (finalrefs.find(cid) == finalrefs.end())
                finalrefs.insert(make_pair(cid, 1));
            else 
                finalrefs[cid]++;

            int bid = cid/ecw;
            
            if (bid <= ecn-1) {
                // final unit needs data read from disk
                if (readbid2cids.find(bid) == readbid2cids.end()) {
                    vector<int> tmplist = {cid};
                    readbid2cids.insert(make_pair(bid, tmplist));
                } else {
                    vector<int> tmplist = readbid2cids[bid];
                    if (find(tmplist.begin(), tmplist.end(), cid) == tmplist.end())
                        readbid2cids[bid].push_back(cid);
                }
            }
        } 
    }

    // 3. repair by transfer
    if (ip2transferunits.size() > 0) {

        for (auto item: ip2transferunits) {
            unsigned int ip = item.first;
            //cout << "ip: " << RedisUtil::ip2Str(ip) << endl;
            vector<int> units = item.second;
            string blockname;

            // figure out read idx and compute tasks
            vector<int> readIdxList;
            vector<ComputeTask*> ctlist;
            int blockidx = -1;
            
            unordered_map<int, int> cacherefs;

            vector<int> parentlist;
            for (auto unitidx: units) {
                ECUnit* cunit = _ecUnitMap[unitidx];
                vector<int> srclist = cunit->getChilds();

                //cout << "srclist: ";
                //for (auto tmpid: srclist)
                //    cout << tmpid << " ";
                //cout << endl;

                if (blockidx == -1) {
                    blockidx = srclist[0]/ecw;
                    for (auto cid: srclist)
                        readIdxList.push_back(cid);
                    blockname = blocklist[blockidx];
                    //cout << "blockidx: " << blockidx << ", blockname: " << blockname << endl;
                } else {
                    assert(blockidx == srclist[0]/ecw);
                    for (auto cid: srclist) {
                        if (find(readIdxList.begin(), readIdxList.end(), cid) == readIdxList.end())
                            readIdxList.push_back(cid);
                    }
                }

                int parent = cunit->getParent();
                vector<int> coef = cunit->getCoefs();
                vector<int> dstlist = {parent};
                vector<vector<int>> coeflist;
                coeflist.push_back(coef);
                ComputeTask* ct = new ComputeTask(srclist, dstlist, coeflist);
                ctlist.push_back(ct);

                assert(finalrefs.find(parent) != finalrefs.end());
                //cacherefs.insert(make_pair(parent, finalrefs[parent]));
                cacherefs.insert(make_pair(parent, 1));

                if (ip2cidlist.find(ip) == ip2cidlist.end()) {
                    vector<int> tmplist = {parent};
                    ip2cidlist.insert(make_pair(ip, tmplist));
                } else {
                    ip2cidlist[ip].push_back(parent);
                }
            }
            sort(readIdxList.begin(), readIdxList.end());

            ECTask* readComputeTask = new ECTask();
            readComputeTask->buildReadCompute(5, ip, blockname, blkbytes, pktbytes, readIdxList,
                    ecw, stripename, ctlist, cacherefs);
            
            tasklist.push_back(readComputeTask);
        }
    }

    // 4. create read disk tasks for real leaves
    //cout << "readbid2cids.size: " << readbid2cids.size() << endl;
    //for (auto item: bid2cids) {
    for (auto item: readbid2cids) {
        int bid = item.first;
        vector<int> cidlist = item.second;
        sort(cidlist.begin(), cidlist.end());
        //cout << "bid: " << bid << ": ";
        //for (auto cid: cidlist)
        //    cout << cid << " ";
        //cout << endl;

        unordered_map<int, int> cid2refs;
        for (auto cid: cidlist) {
            cid2refs.insert(make_pair(cid, 1));
        }

        ECTask* readtask = new ECTask();
        readtask->buildReadDisk(0, loclist[bid], blocklist[bid], 
                blkbytes, pktbytes, ecw, cid2refs, stripename);
        tasklist.push_back(readtask);

        // fill in ip2cidlist
        unsigned int ip = loclist[bid];
        if (ip2cidlist.find(ip) == ip2cidlist.end())
            ip2cidlist.insert(make_pair(loclist[bid], cidlist));
        else {
            for (auto cid: cidlist)
                ip2cidlist[ip].push_back(cid);
        }
    }

    // 2. update shortening in ip2cidlist
    sort(shorteninglist.begin(), shorteninglist.end());
    if (shorteninglist.size() > 0)
        ip2cidlist.insert(make_pair(0, shorteninglist));

    // 3. create compute tasks
    vector<ComputeTask*> ctlist;
    //for (int i=0; i<_ecUnitList.size(); i++) {
    for (int i=0; i<finalunits.size(); i++) {
        //int unitIdx = _ecUnitList[i];
        int unitIdx = finalunits[i];
        ECUnit* cunit = _ecUnitMap[unitIdx];

        vector<int> srclist = cunit->getChilds();
        int parent = cunit->getParent();
        if (parent == REQUESTOR)
            continue;
        vector<int> coef = cunit->getCoefs();

        vector<int> dstlist = {parent};
        vector<vector<int>> coeflist;
        coeflist.push_back(coef);

        ComputeTask* ct = new ComputeTask(srclist, dstlist, coeflist);
        ctlist.push_back(ct);
    }

    unordered_map<int, int> cid2refs;
    vector<int> concatenatelist;
    for (int i=0; i<ecw; i++) {
        int cid = repairIdx * ecw + i;
        cid2refs.insert(make_pair(cid, 1));
        concatenatelist.push_back(cid);
    }
    
    // debug ip2cidlist
    for (auto item: ip2cidlist) {
        unsigned int ip = item.first;
        vector<int> list = item.second;
        //cout << "ip: " << RedisUtil::ip2Str(ip) << ": ";
        //for (auto cid: list)
        //    cout << cid << " ";
        //cout << endl;
    }

    // 4. create fetch and compute
    ECTask* fetchComputeTask = new ECTask();
    fetchComputeTask->buildFetchCompute2(3, loclist[repairIdx], ip2cidlist, ctlist,
            stripename, cid2refs, ecw, blkbytes, pktbytes);
    tasklist.push_back(fetchComputeTask);

    // 5. create concatenate
    unordered_map<unsigned int, vector<int>> ip2conlist;
    ip2conlist.insert(make_pair(loclist[repairIdx], concatenatelist));
    ECTask* concatenateTask = new ECTask();
    concatenateTask->buildConcatenate2(4, loclist[repairIdx],ip2conlist,stripename, 
            blocklist[repairIdx], ecw, blkbytes, pktbytes);
    tasklist.push_back(concatenateTask);

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
}
