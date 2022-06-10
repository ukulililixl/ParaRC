#ifndef _ECCLUSTER_HH_
#define _ECCLUSTER_HH_

#include "../inc/include.hh"
#include "ECUnit.hh"

using namespace std;

class ECCluster {
  private: 
    int _clusterId;
    vector<int> _unitList;
    int _color;

    vector<int> _childList;
    vector<int> _parentList;
    unordered_map<int, vector<int>> _coefMap;

  public:
    ECCluster(int clusterid, vector<int> unitlist);
    ~ECCluster();

    int getClusterId();
    vector<int> getUnitList();
    void setChildList(vector<int> childlist);
    void setParentList(vector<int> parentlist);
    void setCoefMap(unordered_map<int, vector<int>> coefmap);
    bool isConcact(int requestor);
    vector<int> getChildList();
    vector<int> getParentList();
    unordered_map<int, vector<int>> getCoefMap();

    string dump();
};

#endif
