#ifndef _AGCOMMAND_HH_
#define _AGCOMMAND_HH_

#include "../inc/include.hh"
#include "../util/RedisUtil.hh"

using namespace std;

class ComputeTask {
  public:
    vector<int> _srclist;
    vector<int> _dstlist;
    vector<vector<int>> _coefs;

    ComputeTask(vector<int> srclist, vector<int> dstlist, vector<vector<int>> coefs) {
      _srclist = srclist;
      _dstlist = dstlist;
      _coefs = coefs;
    }
    ~ComputeTask() {
    }
};

/*
 * AGCommand Format
 * agent_request: type
 * type=0 (readDisk) | blockname | ecw | indices | stripename |
 */

class AGCommand {
  private:
    char* _agCmd = 0;
    int _cmLen = 0;
    string _rKey;
    int _type;
    unsigned _sendIp;
    int _blkbytes;
    int _pktbytes;

    // type 0
    string _blockName;
    int _ecw; 
    unordered_map<int, int> _cid2refs;
    vector<int> _indices;
    string _stripeName;

    // type 1
    vector<int> _prevIndices;
    vector<unsigned int> _prevLocs;
    vector<ComputeTask*> _ctlist;
    // _stripeName
    // _indices
    // _ecw

    // type 2
    // _prevIndices
    // _prevLocs
    // _stripeName
    // _blockName

    // type 3
    unordered_map<unsigned int, vector<int>> _ip2cidlist;
    int _taskid;
    int _nFetchStream;
    int _nCompute;
    int _nOutCids;
    // _ctlist
    // _stripeName
    // _cid2refs
    // _ecw
    // _blkbytes
    // _pktbytes

    // type 5: repair by transfer
    //

    // type 6: read with offset
    int _offset;

  public:
    AGCommand();
    ~AGCommand();
    AGCommand(char* reqStr);

    // basic construction methods
    void writeInt(int value);
    void writeString(string s);
    int readInt();
    string readString();

    char* getCmd();
    int getCmdLen();
    int getType();
    unsigned int getSendIp();
    string getBlockName();
    int getECW();
    vector<int> getIndices();
    string getStripeName();
    vector<int> getPrevIndices();
    vector<unsigned int> getPrevLocs();
    vector<ComputeTask*> getCTList();
    unordered_map<int, int> getCid2Refs();
    int getBlkBytes();
    int getPktBytes();
    int getTaskid();
    int getNFetchStream();
    int getNCompute();
    int getNOutCids();
    int getOffset();

    void sendTo(unsigned int ip);

    void buildType0(int type, 
            unsigned int sendip, 
            string blockname, 
            int blkbytes,
            int pktbytes,
            int ecw, 
            unordered_map<int, int> cid2ref,
            string stripename);
    void buildType1(int type, 
            unsigned int sendip, 
            vector<int> prevIndices, 
            vector<unsigned int> prevLocs, 
            vector<ComputeTask*> ctlist, 
            string stripename, 
            //vector<int> cacheIndices,
            unordered_map<int, int> cid2refs,
            int ecw,
            int blkbytes,
            int pktbytes);
    void buildType2(int type, 
            unsigned int sendip, 
            vector<int> prevIndices, 
            vector<unsigned int> prevLocs,
            string stripename, 
            string blockname, 
            int ecw,
            int blkbytes,
            int pktbytes);
    void buildType3(int type,
            unsigned int sendip,
            unordered_map<unsigned int, vector<int>> ip2cidlist,
            vector<ComputeTask*> ctlist,
            string stripename,
            unordered_map<int, int> cid2refs,
            int ecw,
            int blkbytes,
            int pktbytes,
            int taskid);
    void buildType4(int type,
            unsigned int sendip,
            unordered_map<unsigned int, vector<int>> ip2cidlist,
            string stripename,
            string blockname,
            int ecw,
            int blkbytes,
            int pktbytes,
            int taskid);
    void buildType5(int type,
            unsigned int sendip,
            string blockname, 
            int blkbytes,
            int pktbytes,
            vector<int> cidlist,
            int ecw, 
            string stripename,
            vector<ComputeTask*> ctlist,
            unordered_map<int, int> cid2refs,
            int taskid);
    void buildType6(int type, 
            unsigned int sendip, 
            string blockname, 
            int blkbytes,
            int pktbytes,
            int ecw, 
            unordered_map<int, int> cid2ref,
            string stripename,
            int offset);
    void buildType7(int type,
            unsigned int sendip,
            string blockname, 
            int blkbytes,
            int pktbytes,
            vector<int> cidlist,
            int ecw, 
            string stripename,
            vector<ComputeTask*> ctlist,
            unordered_map<int, int> cid2refs,
            int taskid,
            int offset);

    // resolve AGCommand
    void resolveType0();
    void resolveType1();
    void resolveType2();
    void resolveType3();
    void resolveType4();
    void resolveType5();
    void resolveType6();
    void resolveType7();

    string dumpStr();
};

#endif
