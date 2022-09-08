#include "CoorCommand.hh"

CoorCommand::CoorCommand() {
  _coorCmd = (char*)calloc(MAX_COMMAND_LEN, sizeof(char));
  _cmLen = 0;
  _rKey = "coor_request";
}

CoorCommand::~CoorCommand() {
  if (_coorCmd) {
    free(_coorCmd);
    _coorCmd = 0;
  }
  _cmLen = 0;
}

CoorCommand::CoorCommand(char* reqStr) {
  _coorCmd = reqStr;
  _cmLen = 0;

  // parse type
  _type = readInt();

  switch(_type) {
    case 0: resolveType0(); break;
    case 1: resolveType1(); break;
    case 2: resolveType2(); break;
    case 3: resolveType3(); break;
//    case 4: resolveType4(); break;
//    case 5: resolveType5(); break;
//    case 6: resolveType6(); break;
//    case 7: resolveType7(); break;
//    case 8: resolveType8(); break;
//    case 9: resolveType9(); break;
//    case 11: resolveType11(); break;
//    case 12: resolveType12(); break;
//    case 21: resolveType21(); break;
    default: break;
  }
  _coorCmd = nullptr;
  _cmLen = 0;
}

void CoorCommand::writeInt(int value) {
  int tmpv = htonl(value);
  memcpy(_coorCmd + _cmLen, (char*)&tmpv, 4); _cmLen += 4;
}

void CoorCommand::writeString(string s) {
  int slen = s.length();
  int tmpslen = htonl(slen);
  // string length
  memcpy(_coorCmd + _cmLen, (char*)&tmpslen, 4); _cmLen += 4;
  // string
  memcpy(_coorCmd + _cmLen, s.c_str(), slen); _cmLen += slen;
}

int CoorCommand::readInt() {
  int tmpint;
  memcpy((char*)&tmpint, _coorCmd + _cmLen, 4); _cmLen += 4;
  return ntohl(tmpint);
}

int CoorCommand::readRawInt() {
  int tmpint;
  memcpy((char*)&tmpint, _coorCmd + _cmLen, 4); _cmLen += 4;
  return tmpint;
}

string CoorCommand::readString() {
  string toret;
  int slen = readInt();
  char* sname = (char*)calloc(sizeof(char), slen+1);
  memcpy(sname, _coorCmd + _cmLen, slen); _cmLen += slen;
  toret = string(sname);
  free(sname);
  return toret;
}

int CoorCommand::getType() {
  return _type;
}

unsigned int CoorCommand::getClientIp() {
  return _clientIp;
}

string CoorCommand::getBlockName() {
  return _blockName;
}

string CoorCommand::getMethod() {
  return _method;
}

unsigned int CoorCommand::getNodeIp() {
    return _nodeIp;
}

string CoorCommand::getCode() {
    return _code;
}

int CoorCommand::getOffset() {
    return _offset;
}

int CoorCommand::getLength() {
    return _length;
}

void CoorCommand::sendTo(unsigned int ip) {
  redisContext* sendCtx = RedisUtil::createContext(ip);
  redisReply* rReply = (redisReply*)redisCommand(sendCtx, "RPUSH %s %b", _rKey.c_str(), _coorCmd, _cmLen);
  freeReplyObject(rReply);
  redisFree(sendCtx);
}

//void CoorCommand::sendTo(redisContext* sendCtx) {
//  redisReply* rReply = (redisReply*)redisCommand(sendCtx, "RPUSH %s %b", _rKey.c_str(), _coorCmd, _cmLen);
//  freeReplyObject(rReply);
//}

void CoorCommand::buildType0(int type, unsigned int ip, string blockname, string method) {
  // set up corresponding parameters
  _type = type;
  _clientIp = ip;
  _blockName = blockname;
  _method = method;

  // 1. type
  writeInt(_type);
  // 2. ipo
  writeInt(_clientIp);
  // 3. blockname
  writeString(_blockName);
  // 4. method
  writeString(_method);
}

void CoorCommand::resolveType0() {
  // 2. ip
  _clientIp = readInt();
  // 3. filename
  _blockName = readString();
  // 4. method
  _method = readString();
}

void CoorCommand::buildType1(int type, unsigned int ip, unsigned int nodeip, string code, string method) {
  // set up corresponding parameters
  _type = type;
  _clientIp = ip;
  _nodeIp = nodeip;
  _code = code;
  _method = method;

  // 1. type
  writeInt(_type);
  // 2. ipo
  writeInt(_clientIp);
  // 3. nodeip
  writeInt(_nodeIp);
  // 3. code
  writeString(_code);
  // 4. method
  writeString(_method);
}

void CoorCommand::resolveType1() {
  // 2. ip
  _clientIp = readInt();
  // 3. nodeip
  _nodeIp = readInt();
  // 4. code
  _code = readString();
  // 4. method
  _method = readString();
}

void CoorCommand::buildType2(int type, unsigned int ip, string blockname, int offset, int length, string method) {
  // set up corresponding parameters
  _type = type;
  _clientIp = ip;
  _blockName = blockname;
  _offset = offset;
  _length = length;
  _method = method;

  // 1. type
  writeInt(_type);
  // 2. ip
  writeInt(_clientIp);
  // 3. blockName
  writeString(_blockName);
  // 4. offset
  writeInt(_offset);
  // 5. length
  writeInt(_length);
  // 6. method
  writeString(_method);
}

void CoorCommand::resolveType2() {
  // 2. ip
  _clientIp = readInt();
  // 3. blockName
  _blockName = readString();
  // 4. offset
  _offset = readInt();
  // 5. lenght
  _length = readInt();
  // 6. method
  _method = readString();
}

void CoorCommand::buildType3(int type, unsigned int ip, unsigned int nodeip, string code, string method) {
  // set up corresponding parameters
  _type = type;
  _clientIp = ip;
  _nodeIp = nodeip;
  _code = code;
  _method = method;

  // 1. type
  writeInt(_type);
  // 2. ipo
  writeInt(_clientIp);
  // 3. nodeip
  writeInt(_nodeIp);
  // 3. code
  writeString(_code);
  // 4. method
  writeString(_method);
}

void CoorCommand::resolveType3() {
  // 2. ip
  _clientIp = readInt();
  // 3. nodeip
  _nodeIp = readInt();
  // 4. code
  _code = readString();
  // 4. method
  _method = readString();
}

void CoorCommand::dump() {
  cout << "CoorCommand::type: " << _type;
  if (_type == 0) {
    cout << ", client: " << RedisUtil::ip2Str(_clientIp)
         << ", blockname: " << _blockName << endl;
//  } else if (_type == 1) {
//    cout << ", client: " << RedisUtil::ip2Str(_clientIp)
//         << ", filename: " << _filename << ", numOfReplicas: " << _numOfReplicas << endl;
//  } else if (_type == 2) {
//    cout << ", client: " << RedisUtil::ip2Str(_clientIp)
//         << ", filename: " << _filename << endl;
//  } else if (_type == 3) {
//    cout << ", client: " << RedisUtil::ip2Str(_clientIp)
//         << ", filename: " << _filename << endl;
//  } else if (_type == 4) {
//    cout << ", client: " << RedisUtil::ip2Str(_clientIp)
//         << ", ecpoolid: " << _ecpoolid
//         << ", stripename: " << _stripename << endl;
//  } else if (_type == 6) {
//    cout << ", client: " << RedisUtil::ip2Str(_clientIp)
//         << ", filename: " << _filename << endl;
//  } else if (_type == 7) {
//    cout << ", enable: " << _op << ", ectype: " << _ectype << endl;
  }
}
