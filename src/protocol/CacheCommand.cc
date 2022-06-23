#include "CacheCommand.hh"

CacheCommand::CacheCommand(string stripename,
        unordered_map<int, int> cid2refs,
        int taskid) {
    _stripeName = stripename;
    _cid2refs = cid2refs;
    _taskid = taskid;

    _cacheCmd = (char*)calloc(MAX_COMMAND_LEN, sizeof(char));
    _cmLen = 0;

    _rKey = stripename + ":task" + to_string(taskid) + ":cache";

}

CacheCommand::CacheCommand(char* reqStr) {
    _cacheCmd = reqStr;
    _cmLen = 0;

    // 0. num cid
    int num = readInt();
    for (int i=0; i<num; i++) {
        int first = readInt();
        int second = readInt();
        _cid2refs.insert(make_pair(first, second));
    }
}

CacheCommand::~CacheCommand() {
    if (_cacheCmd)
        free(_cacheCmd);
}

void CacheCommand::writeInt(int value) {
    int tmpv = htonl(value);
    memcpy(_cacheCmd + _cmLen, (char*)&tmpv, 4); _cmLen += 4;        
}                                             
                                                         
void CacheCommand::writeString(string s) {                                                                      
    int slen = s.length();     
    int tmpslen = htonl(slen);
    // string length
    memcpy(_cacheCmd + _cmLen, (char*)&tmpslen, 4); _cmLen += 4;
    // string                        
    memcpy(_cacheCmd + _cmLen, s.c_str(), slen); _cmLen += slen;
}                             
                                         
int CacheCommand::readInt() {
    int tmpint;  
    memcpy((char*)&tmpint, _cacheCmd + _cmLen, 4); _cmLen += 4;
    return ntohl(tmpint);                              
}                            

string CacheCommand::readString() {
    string toret;
    int slen = readInt();
    char* sname = (char*)calloc(sizeof(char), slen+1);
    memcpy(sname, _cacheCmd + _cmLen, slen); _cmLen += slen;
    toret = string(sname);
    free(sname);
    return toret;                            
}

unordered_map<int, int> CacheCommand::getCid2Refs() {
    return _cid2refs;
}

void CacheCommand::setCmd(char* cmd, int len) {
    _cacheCmd = cmd;
    _cmLen = len;
}

void CacheCommand::sendTo(unsigned int ip) {
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* sendReply = (redisReply*)redisCommand(sendCtx, "RPUSH %s %b", _rKey.c_str(), _cacheCmd, _cmLen);
    freeReplyObject(sendReply);
    redisFree(sendCtx);                
}

void CacheCommand::buildCommand() {
    // 0. num cid
    writeInt(_cid2refs.size());
    for (auto item: _cid2refs) {
        writeInt(item.first);
        writeInt(item.second);
    }
}

