#include "FetchCommand.hh"

FetchCommand::FetchCommand(string stripename,
        unsigned int fetchip,
        vector<int> cidlist,
        int taskid) {
    _fetchCmd = (char*)calloc(MAX_COMMAND_LEN, sizeof(char));
    _cmLen = 0;

    _stripeName = stripename;
    _fetchip = fetchip;
    _cidlist = cidlist;
    _taskid = taskid;

    _rKey = _stripeName+":task"+to_string(taskid)+":fetch";
}

FetchCommand::FetchCommand(char* reqStr) {

    _fetchCmd = reqStr;
    _cmLen = 0;
    
    // 0. fetchip
    _fetchip = readInt();
    // 1. cidlist
    int num = readInt();
    for (int i=0; i<num; i++) {
        int cid = readInt();
        _cidlist.push_back(cid);
    }
}

FetchCommand::~FetchCommand() {
    if (_fetchCmd)
        free(_fetchCmd);
}

void FetchCommand::writeInt(int value) {
    int tmpv = htonl(value);
    memcpy(_fetchCmd + _cmLen, (char*)&tmpv, 4); _cmLen += 4; 
}

void FetchCommand::writeString(string s) {
    int slen = s.length(); 
    int tmpslen = htonl(slen);
    // string length
    memcpy(_fetchCmd + _cmLen, (char*)&tmpslen, 4); _cmLen += 4;
    // string
    memcpy(_fetchCmd + _cmLen, s.c_str(), slen); _cmLen += slen;
}

int FetchCommand::readInt() { 
    int tmpint;
    memcpy((char*)&tmpint, _fetchCmd + _cmLen, 4); _cmLen += 4;
    return ntohl(tmpint);
}

string FetchCommand::readString() {
    string toret;
    int slen = readInt();
    char* sname = (char*)calloc(sizeof(char), slen+1);
    memcpy(sname, _fetchCmd + _cmLen, slen); _cmLen += slen; 
    toret = string(sname);
    free(sname);
    return toret;
}

unsigned int FetchCommand::getFetchIp() {
    return _fetchip;
}

vector<int> FetchCommand::getCidList() {
    return _cidlist;
}

void FetchCommand::setCmd(char* cmd, int len) {
    _fetchCmd = cmd;
    _cmLen = len;
}

void FetchCommand::sendTo(unsigned int ip) {
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* sendReply = (redisReply*)redisCommand(sendCtx, "RPUSH %s %b", _rKey.c_str(), _fetchCmd, _cmLen);
    freeReplyObject(sendReply);
    redisFree(sendCtx);
}

void FetchCommand::buildCommand() {
    // 0. fetchip
    writeInt(_fetchip);
    // 1. cidlist
    writeInt(_cidlist.size());
    for (int i=0; i<_cidlist.size(); i++) {
        writeInt(_cidlist[i]);
    }
}
