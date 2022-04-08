#include "LoadVector.hh"

LoadVector::LoadVector(vector<unsigned int> nodelist) {
  _nodeList = nodelist;
  for (auto node: nodelist) {
    _inNum.insert(make_pair(node, 0));
    _outNum.insert(make_pair(node, 0));
    _calNum.insert(make_pair(node, 0));
  } 
}

LoadVector::LoadVector(vector<unsigned int> nodelist,
               unordered_map<unsigned int, int> innum,
               unordered_map<unsigned int, int> outnum,
               unordered_map<unsigned int, int> calnum) {
  _nodeList = nodelist;
  _inNum = innum;
  _outNum = outnum;
  _calNum = calnum;
}

vector<unsigned int> LoadVector::getNodeList() {
  return _nodeList;
}

unordered_map<unsigned int, int> LoadVector::getInNum() {
  return _inNum;
}

unordered_map<unsigned int, int> LoadVector::getOutNum() {
  return _outNum;
}

unordered_map<unsigned int, int> LoadVector::getCalNum() {
  return _calNum;
}

int LoadVector::getInValueFor(unsigned int ip) {
  if (_inNum.find(ip) == _inNum.end())
    return -1;
  else
    return _inNum[ip];
}

int LoadVector::getOutValueFor(unsigned int ip) {
  if (_outNum.find(ip) == _outNum.end())
    return -1;
  else
    return _outNum[ip];
}

int LoadVector::getCalValueFor(unsigned int ip) {
  if (_calNum.find(ip) == _calNum.end())
    return -1;
  else
    return _calNum[ip];
}

void LoadVector::setLoadValue(unordered_map<unsigned int, int> innum, 
                              unordered_map<unsigned int, int> outnum,
                              unordered_map<unsigned int, int> calnum) {
  _inNum.clear();
  for (auto item: innum) {
    unsigned int ip = item.first;
    int v = item.second;
    _inNum.insert(make_pair(ip, v));
  }

  _outNum.clear();
  for (auto item: outnum) {
    unsigned int ip = item.first;
    int v = item.second;
    _outNum.insert(make_pair(ip, v));
  }

  _calNum.clear();
  for (auto item: calnum) {
    unsigned int ip = item.first;
    int v = item.second;
    _calNum.insert(make_pair(ip, v));
  }
}

void LoadVector::updateLoadValue(unordered_map<unsigned int, int> innum,
                                 unordered_map<unsigned int, int> outnum,
                                 unordered_map<unsigned int, int> calnum) {
  for (auto item: innum) {
    unsigned int ip = item.first;
    int v = item.second;
    if (_inNum.find(ip) == _inNum.end())
      _inNum[ip] = v;
    else
      _inNum[ip] += v;
  }

  for (auto item: outnum) {
    unsigned int ip = item.first;
    int v = item.second;
    if (_outNum.find(ip) == _outNum.end())
      _outNum[ip] = v;
    else
      _outNum[ip] += v;
  }

  for (auto item: calnum) {
    unsigned int ip = item.first;
    int v = item.second;
    if (_calNum.find(ip) == _calNum.end())
      _calNum[ip] = v;
    else
      _calNum[ip] += v;
  }
}

void LoadVector::updateInValueFor(unsigned int ip, int value) {
  if (_inNum.find(ip) != _inNum.end()) {
    _inNum[ip] += value;
  }
}

void LoadVector::updateOutValueFor(unsigned int ip, int value) {
  if (_outNum.find(ip) != _outNum.end()) {
    _outNum[ip] += value;
  }
}

void LoadVector::updateCalValueFor(unsigned int ip, int value) {
  if (_calNum.find(ip) != _calNum.end()) {
    _calNum[ip] += value;
  }
}

string LoadVector::dumpStr() {
  string toret = "node	in	out	cal\n";
  for (auto node: _nodeList) {
    toret += RedisUtil::ip2Str(node) + "    " + to_string(_inNum[node]) + "    " + to_string(_outNum[node])  + "    " + to_string(_calNum[node]) + "\n";
  }
  return toret;
}

pair<int, int> LoadVector::getOverhead() {
  int t = 0, c = 0;
  
  for (auto item: _inNum) {
    if (item.second > t)
      t = item.second; 
  }

  for (auto item: _outNum) {
    if (item.second > t)
      t = item.second; 
  }

  for (auto item: _calNum) {
    if (item.second > c)
      c = item.second; 
  }

  return make_pair(t, c);
}

int LoadVector::compare(LoadVector* a, LoadVector* b) {
  // return 1: a is better
  // return -1: b is better
  // return 0: both are the same

  // In this function, we tell which loadvector is better

  // 0. get the overhead of the two loadvector
  pair<int, int> overhead_a = a->getOverhead();
  pair<int, int> overhead_b = b->getOverhead();

  // 1. the one that has less traffic overhead is better
  if (overhead_a.first < overhead_b.first) {
    cout << "a has lower traffic overhead" << endl;
    return 1;
  } else if (overhead_a.first > overhead_b.first) {
    cout << "b has lower traffic overhead" << endl;
    return -1;
  }
    
  // 2. the one that has less calculation overhead is better
  if (overhead_a.second < overhead_b.second) {
    cout << "a has lower calculation overhead" << endl;
    return 1;
  } else if (overhead_a.second > overhead_b.second) {
    cout << "b has lower calculation overhead" << endl;
    return -1;
  }

  // when we go here, the traffic and calculation overhead are the same
  // now we compare the variance of the traffic and calculation distribution

  vector<int> tfc_a, tfc_b;
  vector<int> cal_a, cal_b;
 
  for (auto item: a->getInNum())
    tfc_a.push_back(item.second);
  for (auto item: a->getOutNum())
    tfc_a.push_back(item.second);
  for (auto item: a->getCalNum())
    cal_a.push_back(item.second);

  for (auto item: b->getInNum())
    tfc_b.push_back(item.second);
  for (auto item: b->getOutNum())
    tfc_b.push_back(item.second);
  for (auto item: b->getCalNum())
    cal_b.push_back(item.second);

  // compare traffic
  double avg_a, avg_b; 
  double via_a, via_b;
  avg_a = DistUtil::average(tfc_a); via_a = DistUtil::variance(tfc_a, avg_a);
  avg_b = DistUtil::average(tfc_b); via_b = DistUtil::variance(tfc_b, avg_b);
  
  cout << "traffic: avg_a = " << avg_a << ", via_a: " << via_a << endl;
  cout << "traffic: avg_b = " << avg_b << ", via_b: " << via_b << endl;

  if (via_a < via_b) {
    cout << "a has lower variance" << endl;
    return 1;
  } else if (via_a > via_b) {
    cout << "b has lower variance" << endl;
    return -1;
  }

  // a and b has the same traffic variance, now we compare the calculation variance
  avg_a = DistUtil::average(cal_a); via_a = DistUtil::variance(cal_a, avg_a);
  avg_b = DistUtil::average(cal_b); via_b = DistUtil::variance(cal_b, avg_b);
  
  cout << "calculation: avg_a = " << avg_a << ", via_a:" << via_a << endl;
  cout << "calculation: avg_b = " << avg_b << ", via_b:" << via_b << endl;

  if (via_a < via_b) {
    cout << "a has lower variance" << endl;
    return 1;
  } else if (via_a > via_b) {
    cout << "b has lower variance" << endl;
    return -1;
  }

  return 0;
}
