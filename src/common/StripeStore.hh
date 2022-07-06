#ifndef _STRIPESTORE_HH_
#define _STRIPESTORE_HH_

//#include "BlockingQueue.hh"
#include "Config.hh"
//#include "SSEntry.hh"
//#include "ECPolicy.hh"
//#include "OfflineECPool.hh"

#include "../inc/include.hh"
//#include "../ec/OfflineECPool.hh"
//#include "../protocol/CoorCommand.hh"
#include "../util/RedisUtil.hh"
#include "../util/DistUtil.hh"
#include "../util/LoadVector.hh"
#include "StripeMeta.hh"
#include "TradeoffPoints.hh"

using namespace std;

class StripeStore {
  private:
    Config* _conf;
    
    // metadata for stripes
    unordered_map<string, StripeMeta*> _stripeMetaMap; 
    unordered_map<string, string> _blk2stripe;
    unordered_map<string, unsigned int> _blk2ip;

    // tradeoff point for codes
    unordered_map<string, TradeoffPoints*> _tradeoffPointsMap;

    // // load vector
    // mutex _lockLoadVector;
    // LoadVector* _loadVector;

  public:
    StripeStore(Config* conf);

    StripeMeta* getStripeMetaFromBlockName(string blockname);
    StripeMeta* getStripeMetaFromStripeName(string stripename);
    TradeoffPoints* getTradeoffPoints(string tpentry);
    unordered_map<string, StripeMeta*> getBlock2StripeMeta(unsigned int nodeip, string code);
    // void lockLoadVector();
    // void unlockLoadVector();
    // void getLoadValueFor(LoadVector* lv);
    // void updateLoadVector(LoadVector* lv);
};

#endif
