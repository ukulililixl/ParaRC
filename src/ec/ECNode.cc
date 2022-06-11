#include "ECNode.hh"

ECNode::ECNode(int id) {
  _nodeId = id;
//  _hasConstraint = false;
//  _consId = -1;
}

ECNode::~ECNode() {
//  for (auto item: _oecTasks) {
//    delete item.second;
//  }
//  _oecTasks.clear();
}

void ECNode::setType(string type) {
  if (type == "leaf")
    _type = 0;
  else if (type == "root")
    _type = 1;
  else
    _type = 2;
}

int ECNode::getNodeId() {
  return _nodeId;
}

void ECNode::setChilds(vector<ECNode*> childs) {
  _childNodes = childs; 
}

void ECNode::setCoefs(vector<int> coefs) {
  _coefs = coefs;
}

void ECNode::addParentNode(ECNode* pnode) {
  _parentNodes.push_back(pnode);
}

int ECNode::getNumChilds() {
  return _childNodes.size();
}

vector<ECNode*> ECNode::getChildNodes() {
  return _childNodes;
}

vector<ECNode*> ECNode::getParentNodes() {
  return _parentNodes;
}

vector<int> ECNode::getCoefs() {
  return _coefs;
}

vector<int> ECNode::getChildIndices() {
  vector<int> toret;
  for (int i=0; i<_childNodes.size(); i++) {
    int idx = _childNodes[i]->getNodeId();
    toret.push_back(idx);
  }
  return toret;
}


void ECNode::dump(int parent) {
  if (parent == -1) parent = _nodeId;
  cout << "(data" << _nodeId;
  if (_childNodes.size() > 0) {
    cout << " = ";
  }
  vector<int> curCoef = _coefs;
//  if (_coefMap.size() > 1) {
//    unordered_map<int, vector<int>>::const_iterator c = _coefMap.find(parent);
//    assert (c!=_coefMap.end());
//    curCoef = _coefMap[parent];
//  } else if (_coefMap.size() == 1) {
//    unordered_map<int, vector<int>>::const_iterator c = _coefMap.find(_nodeId);
//    assert (c!=_coefMap.end());
//    curCoef = _coefMap[_nodeId];
//  }
  for (int i=0; i<_childNodes.size(); i++) {
    cout << curCoef[i] << " ";
    _childNodes[i]->dump(_nodeId);
    if (i < _childNodes.size() - 1) {
      cout << " + ";
    }
  }
  cout << ")";
}

