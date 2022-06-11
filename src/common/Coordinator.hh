#ifndef _COORDINATOR_HH_
#define _COORDINATOR_HH_

#include "Config.hh"
#include "StripeStore.hh"
#include "../ec/ECBase.hh"
#include "../inc/include.hh"
#include "../protocol/AGCommand.hh"
#include "../protocol/CoorCommand.hh"

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
};

#endif
