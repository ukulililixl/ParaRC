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

void ECCluster::setChildList(vector<int> childlist) {
    _childList = childlist;
}

void ECCluster::setParentList(vector<int> parentlist) {
    _parentList = parentlist;
}

void ECCluster::setCoefMap(unordered_map<int, vector<int>> coefmap) {
    _coefMap = coefmap;
}

bool ECCluster::isConcact(int requestor) {
    if (_parentList.size() > 1)
        return false;
    if (_parentList.size() == 1 && _parentList[0] == requestor)
        return true;
    else
        return false;
}

vector<int> ECCluster::getChildList() {
    return _childList;
}

vector<int> ECCluster::getParentList() {
    return _parentList;
}

unordered_map<int, vector<int>> ECCluster::getCoefMap() {
    return _coefMap;
}

string ECCluster::dump() {
  string toret;
  toret += "clusterid: " + to_string(_clusterId) + ";";
  toret += "unitlist: ";
  for (auto item: _unitList)
    toret += to_string(item) + " ";
  return toret;
}
