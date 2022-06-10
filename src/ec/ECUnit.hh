#ifndef _ECUNIT_HH_
#define _ECUNIT_HH_

#include "../inc/include.hh"

using namespace std;

class ECUnit {
  private:
    int _unitId;
    vector<int> _childs;
    int _parent;
    vector<int> _coefs;

  public:
    ECUnit(int unitid, vector<int> childs, int parent, vector<int> coefs);
    int getUnitId();
    vector<int> getChilds();
    int getParent();
    vector<int> getCoefs();

    string dump();
    
};

#endif
