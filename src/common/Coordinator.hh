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
    void repairBlockConv(string blockname, unsigned int clientip, bool enforceip, bool wait); // new protocol
    void repairBlockConv1(string blockname); // old protocol
    void repairBlockDist(string blockname); // new protocol
    void repairBlockDist1(string blockname, unsigned int clientip, bool enforceip, bool wait); // old protocol

    void repairBlockListConv(vector<string> blocklist);
    void repairBlockListDist1(vector<string> blocklist);
    void repairBlockListParaRC(vector<string> blocklist, unordered_map<string, string> blk2solution);

    void repairNode(CoorCommand* coorCmd);
    void repairNodeConv(unsigned int nodeip, string code, unordered_map<string, StripeMeta*> blk2meta);
    void repairNodeDist(unsigned int nodeip, string code, unordered_map<string, StripeMeta*> blk2meta);
    void repairNodeParaRC(unsigned int nodeip, string code, unordered_map<string, StripeMeta*> blk2meta);

    void readBlock(CoorCommand* coorCmd);
    void readBlockConv(string blockname, unsigned int clientip, bool enforceip, int offset, int length); 

    void standbyRepair(CoorCommand* coorCmd);

    unsigned int selectRepairIp(vector<unsigned int> ips);
    int generateLoadTableDist(unordered_map<unsigned int, int>& in, unordered_map<unsigned int, int>& out, string block, StripeMeta* meta);
    int generateLoadTableConv(unordered_map<unsigned int, int>& in, unordered_map<unsigned int, int>& out, string block, StripeMeta* meta);
    void stat(unordered_map<int, int> sidx2ip,
            vector<int> curres, vector<int> itm_idx,
            ECDAG* ecdag, 
            unordered_map<int, int>& in, 
            unordered_map<int, int>& out);
    int testAndTrial(unordered_map<unsigned int, int> inmap,
            unordered_map<unsigned int, int> outmap,
            unordered_map<unsigned int, int> tmpin,
            unordered_map<unsigned int, int> tmpout);
};

#endif
