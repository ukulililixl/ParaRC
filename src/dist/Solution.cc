#include "Solution.hh"

Solution::Solution(bool ishead) {
    _prev = NULL;
    _next = NULL;
    _expanded = false;
    _v = 0;
    _m = 0;

    if (ishead)
        _type = 0;
    else
        _type = 1;
}

Solution::Solution(int v, int m) {
    _v = v;
    _m = m;
    _expanded = false;

    srand((unsigned)time(NULL));
    //srand(1);
    for (int i=0; i<v; i++) {
        int r = rand() % _m;
        _solution.push_back(r);
    }

    //_solution.push_back(3);
    //_solution.push_back(5);
    //_solution.push_back(0);

    _type = 2;

}

Solution::Solution(int v, int m, vector<int> sol) {
    _v = v;
    _m = m;
    _expanded = false;

    for (int i=0; i<sol.size(); i++) {
        int a = sol[i];
        _solution.push_back(a);
    }
    _type = 2;
}

Solution::~Solution() {

}

void Solution::setNext(Solution* sol) {
    _next = sol;
}

Solution* Solution::getNext() {
    return _next;
}

void Solution::setPrev(Solution* sol) {
    _prev = sol;
}

Solution* Solution::getPrev() {
    return _prev;
}

string Solution::getString() {
    // we transfer _solution to a string
    string toret;
    int digits = DistUtil::ndigits(_m);

    for (int i=0; i<_v; i++) {
        toret += DistUtil::num2str(_solution[i], digits);
    }

    //cout << "s: " << toret << endl;

    return toret;
}

void Solution::setExpanded(bool flag) {
    _expanded = flag;
}

bool Solution::getExpanded() {
    return _expanded;
}

vector<int> Solution::getSolution() {
    return _solution;
}

void Solution::setBdwt(int bdwt) {
    _bdwt = bdwt;
}

int Solution::getBdwt() {
    return _bdwt;
}

void Solution::setLoad(int load) {
    _load = load;
}

int Solution::getLoad() {
    return _load;
}

int Solution::getDigits() {
    return DistUtil::ndigits(_m);
}

bool Solution::isHead() {
    if (_type == 0)
        return true;
    else
        return false;
}

bool Solution::isTail() {
    if (_type == 1)
        return true;
    else
        return false;
}
