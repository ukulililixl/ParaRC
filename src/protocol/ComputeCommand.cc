#include "ComputeCommand.hh"

ComputeCommand::ComputeCommand(string stripename,
        vector<int> srclist,
        vector<int> dstlist,
        vector<vector<int>> coefs,
        int taskid) {
    _stripeName = stripename;
    _srclist = srclist;
    _dstlist = dstlist;
    _coefs = coefs;
    _taskid = taskid;

    _computeCmd = (char*)calloc(MAX_COMMAND_LEN, sizeof(char));
    _cmLen = 0; 

    _rKey = stripename + ":task" + to_string(taskid) + ":compute";
}

ComputeCommand::ComputeCommand(char* reqStr) {
    _computeCmd = reqStr;
    _cmLen = 0;

    // 0. srclist
    int srcnum = readInt();
    for (int i=0; i<srcnum; i++) {
        int srcid = readInt();
        _srclist.push_back(srcid);
    }
    // 1. dstlist
    int dstnum = readInt();
    for (int i=0; i<dstnum; i++) {
        int dstid = readInt();
        _dstlist.push_back(dstid);
        vector<int> coef;
        for (int j=0; j<srcnum; j++) {
            int c = readInt();
            coef.push_back(c);
        }
        _coefs.push_back(coef);
    }
}

ComputeCommand::~ComputeCommand() {
    if (_computeCmd)
        free(_computeCmd);
}

void ComputeCommand::writeInt(int value) {
    int tmpv = htonl(value);
    memcpy(_computeCmd + _cmLen, (char*)&tmpv, 4); _cmLen += 4; 
}

void ComputeCommand::writeString(string s) {
    int slen = s.length(); 
    int tmpslen = htonl(slen);
    // string length
    memcpy(_computeCmd + _cmLen, (char*)&tmpslen, 4); _cmLen += 4;
    // string
    memcpy(_computeCmd + _cmLen, s.c_str(), slen); _cmLen += slen;
}

int ComputeCommand::readInt() {
    int tmpint;
    memcpy((char*)&tmpint, _computeCmd + _cmLen, 4); _cmLen += 4;
    return ntohl(tmpint);            
}

string ComputeCommand::readString() {
    string toret;
    int slen = readInt();
    char* sname = (char*)calloc(sizeof(char), slen+1);
    memcpy(sname, _computeCmd + _cmLen, slen); _cmLen += slen; 
    toret = string(sname);
    free(sname);
    return toret;                            
}

vector<int> ComputeCommand::getSrcList() {
    return _srclist;
}

vector<int> ComputeCommand::getDstList() {
    return _dstlist;
}

vector<vector<int>> ComputeCommand::getCoefs() {
    return _coefs;
}

void ComputeCommand::setCmd(char* cmd, int len) {
    _computeCmd = cmd;
    _cmLen = len;
}

void ComputeCommand::sendTo(unsigned int ip) {
    redisContext* sendCtx = RedisUtil::createContext(ip);
    redisReply* sendReply = (redisReply*)redisCommand(sendCtx, "RPUSH %s %b", _rKey.c_str(), _computeCmd, _cmLen);
    freeReplyObject(sendReply);
    redisFree(sendCtx);                
}

void ComputeCommand::buildCommand() {
    // 0. srclist
    writeInt(_srclist.size());
    for (int i=0; i<_srclist.size(); i++)
        writeInt(_srclist[i]);
    // 1. dstlist
    writeInt(_dstlist.size());
    for (int i=0; i<_dstlist.size(); i++) {
        writeInt(_dstlist[i]);
        // coef
        vector<int> coef = _coefs[i];
        for (int j=0; j<_srclist.size(); j++)
            writeInt(coef[j]);
    }
}
