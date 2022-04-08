#ifndef _LOADVECTOR_HH_
#define _LOADVECTOR_HH_

#include "../inc/include.hh"
#include "RedisUtil.hh"
#include "DistUtil.hh"

using namespace std;

class LoadVector {
  private:
    vector<unsigned int> _nodeList;
    unordered_map<unsigned int, int> _inNum;
    unordered_map<unsigned int, int> _outNum; 
    unordered_map<unsigned int, int> _calNum;

  public:
    LoadVector(vector<unsigned int> nodelist); 
    LoadVector(vector<unsigned int> nodelist,
               unordered_map<unsigned int, int> innum,
               unordered_map<unsigned int, int> outnum,
               unordered_map<unsigned int, int> calnum);

    vector<unsigned int> getNodeList();
    unordered_map<unsigned int, int> getInNum();
    unordered_map<unsigned int, int> getOutNum();
    unordered_map<unsigned int, int> getCalNum();
    int getInValueFor(unsigned int ip);
    int getOutValueFor(unsigned int ip);
    int getCalValueFor(unsigned int ip);
    void setLoadValue(unordered_map<unsigned int, int> innum,
                         unordered_map<unsigned int, int> outnum,
                         unordered_map<unsigned int, int> calnum);
    void updateLoadValue(unordered_map<unsigned int, int> innum,
                         unordered_map<unsigned int, int> outnum,
                         unordered_map<unsigned int, int> calnum);
    void updateInValueFor(unsigned int ip, int value);
    void updateOutValueFor(unsigned int ip, int value);
    void updateCalValueFor(unsigned int ip, int value);
    
    pair<int, int> getOverhead();
    static int compare(LoadVector* a, LoadVector* b);
   
    
    string dumpStr();
};

#endif
