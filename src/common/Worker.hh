#ifndef _WORKER_HH_
#define _WORKER_HH_

#include "Config.hh"
#include "DataPacket.hh"
//
#include "../ec/Computation.hh"
#include "../inc/include.hh"
#include "../protocol/AGCommand.hh"
#include "../protocol/FetchCommand.hh"
#include "../protocol/ComputeCommand.hh"
#include "../protocol/CacheCommand.hh"
#include "../util/BlockingQueue.hh"
#include "../util/DistUtil.hh"


class Worker {
  private: 
    int _id;
    Config* _conf;

    redisContext* _processCtx;
    redisContext* _localCtx;
    redisContext* _coorCtx;

    int _tasknum;

  public:
    Worker(Config* conf, int id);
    ~Worker();
    void doProcess();
    
    // deal with coor instruction
    void readAndCache(AGCommand* agCmd);
    void fetchAndCompute(AGCommand* agCmd);
    void fetchAndCompute2(AGCommand* agCmd);
    void debugFetchAndCompute(AGCommand* agCmd);
    void concatenate(AGCommand* agCmd);
    void concatenate2(AGCommand* agCmd);
    void readAndCompute(AGCommand* agCmd);

    void readAndCacheWithOffset(AGCommand* agCmd);
    void readAndComputeWithOffset(AGCommand* agCmd);
    void fetchAndCompute3(AGCommand* agCmd); // for large w
    void fetchAndCompute4(AGCommand* agCmd); // for large w

    // basic routines
    void readWorker(BlockingQueue<DataPacket*>* readqueue, string blockname, int ecw,
                    vector<int> pattern, int blkbytes, int pktbytes);
    void readWorkerWithOffset(BlockingQueue<DataPacket*>* readqueue, string blockname, int ecw,
                    vector<int> pattern, int blkbytes, int pktbytes, int offset);
    void readWorker(unordered_map<int, BlockingQueue<DataPacket*>*> readmap,
            string blockname, int ecw, vector<int> pattern, vector<int> patternidx,
            int blkbytes, int pktbytes);
    void readWorkerWithOffset(unordered_map<int, BlockingQueue<DataPacket*>*> readmap,
            string blockname, int ecw, vector<int> pattern, vector<int> patternidx,
            int blkbytes, int pktbytes, int offset);
    void cacheWorker(BlockingQueue<DataPacket*>* cachequeue,
                     vector<int> idxlis, int ecw, string keybase,
                     int blkbytes, int pktbytes, unordered_map<int, int> cid2refs);
    void cacheWorker2(unordered_map<int, BlockingQueue<DataPacket*>*> cacheMap,
                     unordered_map<int, int> cacheRefs,
                     int ecw, string stripename,
                     int blkbytes, int pktbytes);
    void cacheWorker3(BlockingQueue<DataPacket*>* cachequeue,
                     vector<int> cachelist,
                     unordered_map<int, int> cacheRefs,
                     int ecw, string stripename,
                     int blkbytes, int pktbytes);
    void fetchWorker(BlockingQueue<DataPacket*>* fetchQueue,
                     string keybase,
                     unsigned int loc,
                     int ecw,
                     int blkbytes, 
                     int pktbytes);
    void fetchWorker2(unordered_map<int, BlockingQueue<DataPacket*>*> fetchMap,
                     string stripename,
                     unsigned int loc,
                     int ecw,
                     int blkbytes, 
                     int pktbytes);
    void fetchWorker3(BlockingQueue<DataPacket*>* fetchqueue,
                    string stripename,
                    vector<int> cidlist,
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
    void computeWorker2(unordered_map<int, BlockingQueue<DataPacket*>*> fetchMap,
            unordered_map<int, BlockingQueue<DataPacket*>*> cacheMap,
            vector<ComputeTask*> ctlist, int ecw, int blkbytes, int pktbytes);
    void computeWorker3(unordered_map<unsigned int, BlockingQueue<DataPacket*>*> fetchMap,
            unordered_map<unsigned int, vector<int>> cidmap,
            BlockingQueue<DataPacket*>* cachequeue, 
            vector<int> cachelist,
            vector<ComputeTask*> ctlist, int ecw, int blkbytes, int pktbytes);
};

#endif
