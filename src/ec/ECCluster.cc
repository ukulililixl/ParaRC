#include "ECCluster.hh"

ECCluster::ECCluster(int clusterid, vector<int> unitlist) {
  _clusterId = clusterid;
  _unitList = unitlist;
}

ECCluster::~ECCluster() {
}

int ECCluster::getClusterId() {
  return _clusterId;
}

vector<int> ECCluster::getUnitList() {
  return _unitList;
}

string ECCluster::dump() {
  string toret;
  toret += "clusterid: " + to_string(_clusterId) + ";";
  toret += "unitlist: ";
  for (auto item: _unitList)
    toret += to_string(item) + " ";
  return toret;
}
