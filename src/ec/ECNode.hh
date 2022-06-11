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

  public:
    ECNode(int id);
    ~ECNode();

    void setType(string type);

    int getNodeId();

    void setChilds(vector<ECNode*> childs);
    void setCoefs(vector<int> coefs);
    void addParentNode(ECNode* pnode);

    int getNumChilds();
    vector<ECNode*> getChildNodes();
    vector<ECNode*> getParentNodes();
    vector<int> getCoefs();
    vector<int> getChildIndices();
    
    // for debug
    void dump(int parent);
};

#endif
