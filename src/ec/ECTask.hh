#ifndef _ECTASK_HH_
#define _ECTASK_HH_

#include "../inc/include.hh"
#include "../util/RedisUtil.hh"
#include "../protocol/AGCommand.hh"

using namespace std;

/*
 * Type of ECTasks
 * 0: read from disk and cache locally
 * 1: fetch from several remote nodes, perform computation locally (if any), and cache result locally
 * 2: fetch from several remote nodes, concactenate them and persist to disk
 */

class ECTask {

  private:
    int _type; 
    unsigned int _loc;

    // for type 0
    string _blockName;
    int _blkBytes;
    int _pktBytes;
    int _ecw; // sub-packetization
    unordered_map<int, int> _cid2refs;
    string _stripeName;

    // for type 1
    vector<int> _prevIndices;
    vector<unsigned int> _prevLocs;
    vector<ComputeTask*> _computeTaskList;
    // stripeName
    //vector<int> _indices;
    //_cid2refs;
    // _ecw
    // _blkBytes
    // _pktBytes

    // for type 2
    //_prevIndices;
    //_prevLocs;
    //_stripeName
    // _blockName

  public:
    ECTask();
    ~ECTask();

    void buildReadDisk(int type,
            unsigned int loc,
            string blockname,
            int blkbytes,
            int pktbytes,
            int ecw,
            unordered_map<int, int> cid2ref,
            string stripename);
    void buildFetchCompute(int type,
               unsigned int loc,
               vector<int> prevIndices,
               vector<unsigned> prevLocs,
               vector<ComputeTask*> computelist,
               string stripename,
               //vector<int> indices,
               unordered_map<int, int> cid2refs,
               int ecw, int blkbytes, int pktbytes);
    void buildConcatenate(int type,
               unsigned int loc,
               vector<int> prevIndices,
               vector<unsigned int> prevLocs,
               string stripename,
               string blockname,
               int ecw, int blkbytes, int pktbytes);

    int getType();
    vector<ComputeTask*> getComputeTaskList();
    string dumpStr();
    AGCommand* genAGCommand();
};

#endif
