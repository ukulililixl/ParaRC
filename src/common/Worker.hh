#ifndef _WORKER_HH_
#define _WORKER_HH_

#include "Config.hh"
#include "DataPacket.hh"
//
#include "../ec/Computation.hh"
#include "../inc/include.hh"
#include "../protocol/AGCommand.hh"
#include "../util/BlockingQueue.hh"
#include "../util/DistUtil.hh"


class Worker {
  private: 
    int _id;
    Config* _conf;

    redisContext* _processCtx;
    redisContext* _localCtx;
    redisContext* _coorCtx;

  public:
    Worker(Config* conf, int id);
    ~Worker();
    void doProcess();
    
    // deal with coor instruction
    void readAndCache(AGCommand* agCmd);
    void fetchAndCompute(AGCommand* agCmd);
    void debugFetchAndCompute(AGCommand* agCmd);
    void concatenate(AGCommand* agCmd);

    // basic routines
    void readWorker(BlockingQueue<DataPacket*>* readqueue, string blockname, int ecw,
                    vector<int> pattern, int blkbytes, int pktbytes);
    void cacheWorker(BlockingQueue<DataPacket*>* cachequeue,
                     vector<int> idxlis, int ecw, string keybase,
                     int blkbytes, int pktbytes, unordered_map<int, int> cid2refs);
    void fetchWorker(BlockingQueue<DataPacket*>* fetchQueue,
                     string keybase,
                     unsigned int loc,
                     int ecw,
                     int blkbytes, 
                     int pktbytes);
    void computeWorker(BlockingQueue<DataPacket*>** fetchQueue,
                       vector<int> fetchIndices,
                       BlockingQueue<DataPacket*>** writeQueue,
                       vector<int> writeIndices,
                       vector<ComputeTask*> ctlist, int ecw,
                       int blkbytes, int pktbytes);
};

#endif
