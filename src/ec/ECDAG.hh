#ifndef _ECDAG_HH_
#define _ECDAG_HH_

#include "../inc/include.hh"
#include "../protocol/AGCommand.hh"
//
//#include "Cluster.hh"
#include "ECNode.hh"
#include "ECUnit.hh"
#include "ECCluster.hh"
#include "ECTask.hh"
#include "../util/BlockingQueue.hh"
#include "../util/LoadVector.hh"

using namespace std;

#define ECDAG_DEBUG_ENABLE true
//#define BINDSTART 200
//#define OPTSTART 300

#define REQUESTOR 32767
#define SGSTART 0
#define USTART 0
#define CSTART 0

class ECDAG {
  private:
    unordered_map<int, ECNode*> _ecNodeMap;
    vector<int> _ecHeaders;
    vector<int> _ecLeaves;

    // // for SimpleGraph
    // int _sgId = SGSTART;
    // unordered_map<int, SimpleGraph*> _sgMap;
    // vector<int> _sgList;
    // unordered_map<int, vector<int>> _child2sgs;

    // for coloring
    unordered_map<int, unsigned int> _idx2ip;

    // for ECUnits
    int _unitId = USTART;
    unordered_map<int, ECUnit*> _ecUnitMap;
    vector<int> _ecUnitList;

    // for ECClusters
    int _clusterId = CSTART;
    unordered_map<int, ECCluster*> _ecClusterMap;
    vector<int> _ecClusterList;
    
  public:
    ECDAG(); 
    ~ECDAG();

    void Join(int pidx, vector<int> cidx, vector<int> coefs);
    void Concact(vector<int> cidx);

    //void genSimpleGraph();
    //void initializeLeaveIp(unordered_map<int, unsigned int> availIps);
    //void initializeRequestorIp(unsigned int ip);
    //void initializeIntermediateIp(unsigned int ip); 
    //void fillLoadVector(LoadVector* loadvector);
    //void coloring(LoadVector* loadvector);
    //void genECTasks(vector<ECTask*>& tasklist,
    //                int ecn, int eck, int ecw,
    //                string stripename, vector<string> blocklist); 
    //void simulateLoc();

    void genECUnits();
    void clearECCluster();
    void genECCluster(unordered_map<int, int> coloring);
    void genStat(unordered_map<int, int> coloring, unordered_map<int, int>& inmap, unordered_map<int, int>& outmap);
    void genECTasksByECClusters(vector<ECTask*>& tasklist,
            int ecn, int eck, int ecw, int blkbytes, int pktbytes,
            string stripename, vector<string> blocklist,
            unordered_map<int, unsigned int> coloring_res);
    void genComputeTaskByECUnits(vector<ComputeTask*>& tasklist);

    unordered_map<int, ECNode*> getECNodeMap();
    vector<int> getECHeaders();
    vector<int> getECLeaves();
    unordered_map<int, ECUnit*> getUnitMap();
    vector<int> getUnitList();

    // for debug
    void dump();
};
#endif
