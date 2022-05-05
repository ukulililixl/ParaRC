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

  public:
    ECCluster(int clusterid, vector<int> unitlist);
    ~ECCluster();

    int getClusterId();
    vector<int> getUnitList();

    string dump();
};

#endif
