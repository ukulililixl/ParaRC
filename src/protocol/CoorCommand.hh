#ifndef _COORCOMMAND_HH_
#define _COORCOMMAND_HH_

#include "../inc/include.hh"
#include "../util/RedisUtil.hh"

using namespace std;

/**
 * CoorCommand Format
 * coor_request: type
 *   type = 0: clientip | blockname | method |
 *   type = 1: clientip | nodeip | code | method |
 *   type = 2: clientip | blockname | offset | length | method |
 */


class CoorCommand {
  private:
    char* _coorCmd;
    int _cmLen;
    string _rKey;
    int _type;
    unsigned int _clientIp;

    // type 0 
    string _blockName;
    string _method;

    // type 1
    unsigned int _nodeIp;
    string _code;
    // _method;

    // type 2
    int _offset;
    int _length;


  public:
    CoorCommand();
    ~CoorCommand();
    CoorCommand(char* reqStr);

    // basic construction methods
    void writeInt(int value);
    void writeString(string s);
    int readInt();
    int readRawInt();
    string readString();

    int getType();
    unsigned int getClientIp();
    string getBlockName();
    string getMethod();
    unsigned int getNodeIp();
    string getCode();
    int getOffset();
    int getLength();
    
    // send method
    void sendTo(unsigned int ip);
//    void sendTo(redisContext* sendCtx);
//
    // build CoorCommand
    void buildType0(int type, unsigned int ip, string blockname, string method); 
    void buildType1(int type, unsigned int ip, unsigned int nodeip, string code, string method);
    void buildType2(int type, unsigned int ip, string blockname, int offset, int length, string method);
    void buildType3(int type, unsigned int ip, unsigned int nodeip, string code, string method);
    void buildType4(int type, unsigned int ip, string blockname, string method); 

    // resolve CoorCommand
    void resolveType0();
    void resolveType1();
    void resolveType2();
    void resolveType3();
    void resolveType4();
//
    // for debug
    void dump();
};

#endif
