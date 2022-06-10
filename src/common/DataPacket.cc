#include "DataPacket.hh"

DataPacket::DataPacket() {
}

DataPacket::DataPacket(char* raw) {
  int tmplen;
  memcpy((char*)&tmplen, raw, 4);
  _dataLen = ntohl(tmplen);

  _raw = (char*)calloc(_dataLen+4, sizeof(char));
  memcpy(_raw, raw, _dataLen+4);

  _data = _raw+4;
}

DataPacket::DataPacket(int len) {
  _raw = (char*)calloc(len+4, sizeof(char));
  _data = _raw+4;
  _dataLen = len;

  int tmplen = htonl(len) ;
  memset(_raw, 0, len+4);
  memcpy(_raw, (char*)&tmplen, 4);
}

DataPacket::~DataPacket() {
  if (_raw) free(_raw);
}

void DataPacket::setRaw(char* raw) {
  int tmplen;
  memcpy((char*)&tmplen, raw, 4);
  _dataLen = ntohl(tmplen);

  _raw = raw;
  _data = _raw+4;
}

int DataPacket::getDatalen() {
  return _dataLen;
}

char* DataPacket::getData() {
  return _data;
}

char* DataPacket::getRaw() {
  return _raw;
}
