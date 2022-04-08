#ifndef _STRIPEMETA_HH_
#define _STRIPEMETA_HH_

#include "../inc/include.hh"
#include "../util/tinyxml2.h"
#include "../util/DistUtil.hh"
#include "../ec/ECBase.hh"
#include "../ec/Clay.hh"

using namespace tinyxml2;

class StripeMeta {
  public:
    StripeMeta(std::string& stripename, std::string& filePath);
    ~StripeMeta();

    std::string _stripeName;
    std::string _codeName;
    int _ecN;
    int _ecK;
    int _ecW;
    vector<std::string> _blockList;
    vector<unsigned int> _locList;

    std::string getStripeName();
    int getECN();
    int getECK();
    int getECW();
    vector<std::string> getBlockList();
    vector<unsigned int> getLocList();
    int getBlockIndex(string blockName);
    ECBase* createECClass();
};

#endif
