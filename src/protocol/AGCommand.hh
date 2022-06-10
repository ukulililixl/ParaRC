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
            vector<int> cacheIndices, 
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

    // resolve AGCommand
    void resolveType0();
    void resolveType1();
    void resolveType2();

    string dumpStr();
};

#endif
