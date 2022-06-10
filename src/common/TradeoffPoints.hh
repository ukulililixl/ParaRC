#ifndef _TRADEOFFPOINTS_HH_
#define _TRADEOFFPOINTS_HH_

#include "../inc/include.hh"                                                                                                                                                                               
#include "../util/tinyxml2.h"                                                                                                                                                                              
#include "../util/DistUtil.hh"  

using namespace tinyxml2;

class TradeoffPoints {
    private:
        std::string _codeName;
        int _ecN;
        int _ecK;
        int _ecW;
        int _digits;
        unordered_map<int, string> _points;

    public:
        TradeoffPoints(std::string filepath);
        vector<int> getColoringByIdx(int idx);

};

#endif
