#include "SimpleGraph.hh"

SimpleGraph::SimpleGraph(int sgid, vector<int> child, int parent, vector<int> coef) {
  _sgId = sgid;
  _childs = child;
  _parents.push_back(parent);
  _coefs.insert(make_pair(parent, coef));

  _concact = true;
  for (auto c: coef) {
    if (c != -1) {
      _concact = false;
      break;
    }
  }

  _colored = false;
}

SimpleGraph::~SimpleGraph() {
  for (auto task: _ecTaskList) {
    delete task;
  }
}

bool SimpleGraph::childsMatch(vector<int> childs) {
  if (childs.size() != _childs.size()) 
    return false;
  for (int i=0; i<childs.size(); i++) {
    int curId = childs[i];
    if (find(_childs.begin(), _childs.end(), curId) == _childs.end())
      return false;
  }
  return true;
}

bool SimpleGraph::parentIn(int pid) {
  if (find(_parents.begin(), _parents.end(), pid) == _parents.end())
    return false;
  else
    return true;
}

void SimpleGraph::addParent(int parent, unordered_map<int, int> coefmap) {
  vector<int> curcoefs;
  for (int i=0; i<_childs.size(); i++) {
    curcoefs.push_back(coefmap[_childs[i]]);
  }
  _parents.push_back(parent);
  _coefs.insert(make_pair(parent, curcoefs));
}

vector<int> SimpleGraph::getChilds() {
  return _childs;
}

vector<int> SimpleGraph::getParents() {
  return _parents;
}

unordered_map<int, vector<int>> SimpleGraph::getCoefs() {
  return _coefs;
}

string SimpleGraph::dumpStr() {
  string toret;
  toret += "SG: sgid = " + to_string(_sgId) + "; ";
  toret += "childs: ( ";
  for (auto cid: _childs)
    toret += to_string(cid) + " ";
  toret += "); ";
  toret += "parents: ( ";
  for (auto pid: _parents)
    toret += to_string(pid) + " ";
  toret += "); ";
  return toret;
}

