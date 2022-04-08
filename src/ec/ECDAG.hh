#ifndef _ECDAG_HH_
#define _ECDAG_HH_

#include "../inc/include.hh"
//#include "../protocol/AGCommand.hh"
//
//#include "Cluster.hh"
#include "ECNode.hh"
#include "SimpleGraph.hh"
#include "../util/BlockingQueue.hh"
#include "../util/LoadVector.hh"

using namespace std;

#define ECDAG_DEBUG_ENABLE true
//#define BINDSTART 200
//#define OPTSTART 300

#define REQUESTOR 32767
#define SGSTART 0

class ECDAG {
  private:
    unordered_map<int, ECNode*> _ecNodeMap;
    vector<int> _ecHeaders;
    vector<int> _ecLeaves;

    // for SimpleGraph
    int _sgId = SGSTART;
    unordered_map<int, SimpleGraph*> _sgMap;
    vector<int> _sgList;
    unordered_map<int, vector<int>> _child2sgs;

    // for coloring
    unordered_map<int, unsigned int> _idx2ip;
    
//    vector<Cluster*> _clusterMap;
//    int _bindId = BINDSTART;
//    int _optId = OPTSTART; 

  public:
    ECDAG(); 
    ~ECDAG();

    void Join(int pidx, vector<int> cidx, vector<int> coefs);
    void Concact(vector<int> cidx);
//    int BindX(vector<int> idxs);
//    void BindY(int pidx, int cidx);

    void genSimpleGraph();
    void initializeLeaveIp(unordered_map<int, unsigned int> availIps);
    void initializeRequestorIp(unsigned int ip);
    void initializeIntermediateIp(unsigned int ip); 
    void fillLoadVector(LoadVector* loadvector);
    void coloring(LoadVector* loadvector);
    void genECTasks(vector<ECTask*>& tasklist,
                    int ecn, int eck, int ecw,
                    string stripename, vector<string> blocklist); 
//
//    int BindXAligned(vector<int> idxs);
//    int BindXNotAligned(vector<int> idxs);
//
//    // topological sorting
//    vector<int> toposort();
//    ECNode* getNode(int cidx);
//    vector<int> getHeaders();
//    vector<int> getLeaves();
//
//    // ecdag reconstruction
//    void reconstruct(int opt);
//    void optimize(int opt, 
//                  unordered_map<int, pair<string, unsigned int>> objlist,
//                  unordered_map<unsigned int, string> ip2Rack,
//                  int ecn,
//                  int eck,
//                  int ecw);
//    void optimize2(int opt, 
//                  unordered_map<int, unsigned int>& cid2ip,
//                  unordered_map<unsigned int, string> ip2Rack,
//                  int ecn, int eck, int ecw,
//                  unordered_map<int, unsigned int> sid2ip,
//                  vector<unsigned int> allIps,
//                  bool locality);
//    void Opt0();
//    void Opt1();
//    void Opt2(unordered_map<int, string> n2Rack);
//    void Opt3();
//
//    // parse cmd
//    unordered_map<int, AGCommand*> parseForOEC(unordered_map<int, unsigned int> cid2ip,
//                                   string stripename, 
//                                   int n, int k, int w, int num,
//                                   unordered_map<int, pair<string, unsigned int>> objlist);
//    unordered_map<int, AGCommand*> parseForOECWithVirtualLeaves(unordered_map<int, unsigned int> cid2ip,
//                                   unordered_map<int, int> virleaves,
//                                   string stripename, 
//                                   int n, int k, int w, int num,
//                                   unordered_map<int, pair<string, unsigned int>> objlist);
//    vector<AGCommand*> persist(unordered_map<int, unsigned int> cid2ip, 
//                                  string stripename,
//                                  int n, int k, int w, int num,
//                                  unordered_map<int, pair<string, unsigned int>> objlist);
//
//    vector<AGCommand*> persistWithInfo(unordered_map<int, unsigned int> cid2ip, 
//            string stripename,
//            int n, int k, int w, int num,
//            unordered_map<int, pair<string, unsigned int>> objlist,
//            vector<int> psid);
//    vector<AGCommand*> persistLayerWithInfo(unordered_map<int, unsigned int> cid2ip, 
//            string stripename,
//            int n, int k, int w, int num,
//            unordered_map<int, pair<string, unsigned int>> objlist,
//            vector<int> psid, string layer);
//
    // for debug
    void dump();
};
#endif
