#include "ECUnit.hh"

ECUnit::ECUnit(int unitid, vector<int> childs, int parent, vector<int> coefs) {
  _unitId = unitid;
  _childs = childs;
  _parent = parent;
  _coefs = coefs;
}

int ECUnit::getUnitId() {
  return _unitId;
}

vector<int> ECUnit::getChilds() {
  return _childs;
}

int ECUnit::getParent() {
  return _parent;
}

string ECUnit::dump() {
  string toret;
  toret += "unit " + to_string(_unitId) + "; childs: ";
  for (int i=0; i<_childs.size(); i++) {
    toret += to_string(_childs[i]) + " ";
  }
  return toret;
}
