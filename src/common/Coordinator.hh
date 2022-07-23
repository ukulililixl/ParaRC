#ifndef _COORDINATOR_HH_
#define _COORDINATOR_HH_

#include "Config.hh"
#include "StripeStore.hh"
#include "../ec/ECBase.hh"
#include "../inc/include.hh"
#include "../protocol/AGCommand.hh"
#include "../protocol/CoorCommand.hh"
#include "../util/RedisUtil.hh"

using namespace std;

class Coordinator {
  private:
    Config* _conf;
    redisContext* _localCtx;
    StripeStore* _stripeStore;

  public:
    Coordinator(Config* conf, StripeStore* ss);
    ~Coordinator();

    void doProcess();
    void stat(unordered_map<int, int> sidx2ip,
            vector<int> curres, vector<int> itm_idx,
            ECDAG* ecdag, int* bdwt, int* maxload);

    void repairBlock(CoorCommand* coorCmd);
    void repairBlockConv(string blockname, unsigned int clientip); // new protocol
    void repairBlockConv1(string blockname); // old protocol
    void repairBlockDist(string blockname); // new protocol
    void repairBlockDist1(string blockname, unsigned int clientip); // old protocol

    void repairBlockListConv(vector<string> blocklist);
    void repairBlockListDist1(vector<string> blocklist);

    void repairNode(CoorCommand* coorCmd);
    void repairNodeConv(unsigned int nodeip, string code, unordered_map<string, StripeMeta*> blk2meta);
    void repairNodeDist(unsigned int nodeip, string code, unordered_map<string, StripeMeta*> blk2meta);
};

#endif
