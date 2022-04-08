#ifndef _SIMPLEGRAPH_HH_
#define _SIMPLEGRAPH_HH_

#include "../inc/include.hh"
#include "ECTask.hh"

using namespace std;

class SimpleGraph {
  private:
    int _sgId;
    vector<int> _childs;
    vector<int> _parents;
    unordered_map<int, vector<int>> _coefs;
    bool _concact;
    bool _colored;

    vector<ECTask*> _ecTaskList;

  public:
    SimpleGraph(int sgid, vector<int> child, int parent, vector<int> coef);
    ~SimpleGraph();

    bool childsMatch(vector<int> childs);
    bool parentIn(int parent);
    void addParent(int parent, unordered_map<int, int> coefmap);
    vector<int> getChilds();
    vector<int> getParents();
    unordered_map<int, vector<int>> getCoefs();

    string dumpStr();
  
};

#endif
