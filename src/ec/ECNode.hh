#ifndef _ECNODE_HH_
#define _ECNODE_HH_

//#include "ECTask.hh"
#include "../inc/include.hh"
//#include "../protocol/AGCommand.hh"

#define ECNODE_DEBUG false
using namespace std;

class ECNode {
  private: 
    int _nodeId;

    int _type; // 0: leaf; 1: root; 2: intermediate

    // _childNodes record all children of current node
    vector<ECNode*> _childNodes;
    vector<int> _coefs;
    vector<ECNode*> _parentNodes;
//    // For each <key, value> pair in _coefMap:
//    // key: target _nodeId that we want to compute
//    // value: corresponding coefs to compute key
//    unordered_map<int, vector<int>> _coefMap;
//
//    // _refNo records the number of parents of current node
//    unordered_map<int, int> _refNumFor;
//
//    bool _hasConstraint;
//    int _consId; 
//
//    // oectasks type0:Load; type1:Fetch; type2: Compute; type3: Cache (enabled by default); type4: Persist
//    // virtual task type5: tell fetching from where 
//    unsigned int _ip;
//    unordered_map<int, ECTask*> _oecTasks;

  public:
    ECNode(int id);
    ~ECNode();

    void setType(string type);

    int getNodeId();

//    void cleanChilds();
    void setChilds(vector<ECNode*> childs);
    void setCoefs(vector<int> coefs);
    void addParentNode(ECNode* pnode);

    int getNumChilds();
    vector<ECNode*> getChildNodes();
    vector<ECNode*> getParentNodes();
    vector<int> getCoefs();
//    int getChildNum();
//    vector<ECNode*> getChildren();
//    ECNode* getChildNode(int cid);
//
//    void addCoefs(int calfor, vector<int> coefs);
//    unordered_map<int, vector<int>> getCoefmap();
//    int getCoefOfChildForParent(int child, int parent);
//
//    void incRefNumFor(int id); // increase refnum for id
//    void decRefNumFor(int id);
//    void cleanRefNumFor(int id);
//    int getRefNumFor(int id);
//    void setRefNum(int nid, int ref);
//    unordered_map<int, int> getRefMap();
//
//    void setConstraint(bool cons, int id);
//    bool isOptNode();
//
//    unsigned int getIp();
//
//    // parseForClient compute tasks
//    void parseForClient(vector<ECTask*>& tasks);
//
//    // parseForOEC tasks
//    void parseForOEC(unsigned int ip);
//    void parseForOECWithVirtualLeaves(unsigned int ip, unordered_map<int, int> virleaves, int w);
//    vector<unsigned int> candidateIps(unordered_map<int, unsigned int> sid2ip,
//                           unordered_map<int, unsigned int> cid2ip,
//                           vector<unsigned int> allIps,
//                           int n, int k, int w, bool locality);
//    vector<unsigned int> candidateIps(unordered_map<int, unsigned int> sid2ip,
//                           unordered_map<int, unsigned int> cid2ip,
//                           vector<unsigned int> allIps,
//                           int n, int k, int w, bool locality, int lostid);
//    vector<unsigned int> candidateIpsWithVirtualLeaves(unordered_map<int, unsigned int> sid2ip,
//                           unordered_map<int, unsigned int> cid2ip,
//                           vector<unsigned int> allIps,
//                           unordered_map<int, int> virleaves,
//                           int n, int k, int w, bool locality);
//    vector<unsigned int> candidateIpsWithVirtualLeaves(unordered_map<int, unsigned int> sid2ip,
//                           unordered_map<int, unsigned int> cid2ip,
//                           vector<unsigned int> allIps,
//                           unordered_map<int, int> virleaves,
//                           int n, int k, int w, bool locality, int lostid);
//    unordered_map<int, ECTask*> getTasks();
//    void clearTasks();
//    AGCommand* parseAGCommand(string stripename,
//                              int n, int k, int w,
//                              int num,
//                              unordered_map<int, pair<string, unsigned int>> stripeobjs,
//                              unordered_map<int, unsigned int> cid2ip);
//
    // for debug
    void dump(int parent);
//    void dumpRawTask();
};

#endif
