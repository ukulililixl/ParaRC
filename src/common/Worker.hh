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
    void concatenate(AGCommand* agCmd);

    // basic routines
    void readWorker(BlockingQueue<DataPacket*>* readqueue, string blockname, int ecw,
                    vector<int> pattern);
    void cacheWorker(BlockingQueue<DataPacket*>* cachequeue,
                     vector<int> idxlis, int ecw, string keybase);
    void fetchWorker(BlockingQueue<DataPacket*>* fetchQueue,
                     string keybase,
                     unsigned int loc);
    void computeWorker(BlockingQueue<DataPacket*>** fetchQueue,
                       vector<int> fetchIndices,
                       BlockingQueue<DataPacket*>** writeQueue,
                       vector<int> writeIndices,
                       vector<ComputeTask*> ctlist, int ecw);
//    void fetchCompute(AGCommand* agCmd);
//    void persist(AGCommand* agCmd);
//    void persistLayer(AGCommand* agCmd);
//    void readFetchCompute(AGCommand* agCmd);
//    void shortenGeneration(AGCommand* agCmd);
//
//    void selectCacheWorker(BlockingQueue<OECDataPacket*>* cacheQueue,
//                           int pktnum,
//                           string keybase,
//                           int w,
//                           vector<int> idxlist,
//                           unordered_map<int, int> refs);
//    void partialCacheWorker(BlockingQueue<OECDataPacket*>* cacheQueue,
//                           int pktnum,
//                           string keybase,
//                           int w,
//                           vector<int> idxlist,
//                           unordered_map<int, int> refs);
//    void fetchWorker(BlockingQueue<OECDataPacket*>* fetchQueue,
//                     string keybase,
//                     unsigned int loc,
//                     int num);
//    void computeWorker(BlockingQueue<OECDataPacket*>** fetchQueue,
//                       int nprev,
//                       int num,
//                       unordered_map<int, vector<int>> coefs,
//                       vector<int> cfor,
//                       BlockingQueue<OECDataPacket*>** writeQueue,
//                       int slicesize);
//    void computeWorker(BlockingQueue<OECDataPacket*>** fetchQueue,
//                       int nprev,
//                       vector<int> prevCids,
//                       int num,
//                       unordered_map<int, vector<int>> coefs,
//                       vector<int> cfor,
//		       unordered_map<int, BlockingQueue<OECDataPacket*>*> writeQueue,
//                       int slicesize);
//    void cacheWorker(BlockingQueue<OECDataPacket*>* writeQueue,
//                     string keybase,
//                     int num,
//                     int refs);
//    void cacheWorker(BlockingQueue<OECDataPacket*>* writeQueue,
//                     string keybase,
//                     int startidx,
//                     int num,
//                     int refs);
//    void cacheWorker(BlockingQueue<OECDataPacket*>* writeQueue,
//                     string keybase,
//                     int startidx,
//                     int step,
//                     int num,
//                     int refs);
//
};

#endif
