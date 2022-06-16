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

vector<int> ECUnit::getCoefs() {
    return _coefs;
}

string ECUnit::dump() {
  string toret;
  toret += "unit " + to_string(_unitId) + "; childs: ";
  for (int i=0; i<_childs.size(); i++) {
    toret += to_string(_childs[i]) + " ";
  }
  toret += "; parent: " + to_string(_parent);
  return toret;
}

string ECUnit::getChildStr() {
    // the default child string size is 5
    int digits = 5;
    string toret = "";
    vector<int> tmpchilds;
    for (int i=0; i<_childs.size(); i++) {
        tmpchilds.push_back(_childs[i]);
    }
    sort(tmpchilds.begin(), tmpchilds.end());
    for (int i=0; i<tmpchilds.size(); i++) {
        string curstr = to_string(tmpchilds[i]);
        int zerobytes = digits - curstr.length();
        while (zerobytes > 0) {
            curstr = "0"+curstr;
            zerobytes--;
        }
        toret += curstr;
    }
    return toret;
}
