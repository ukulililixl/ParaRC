#include "RedisUtil.hh"

redisContext* RedisUtil::createContext(unsigned int ip) {
  return createContext(ip2Str(ip), 6379);
}

redisContext* RedisUtil::createContext(string ip) {
  return createContext(ip, 6379);
}

redisContext* RedisUtil::createContext(string ip, int port) {
  redisContext* retVal = redisConnect(ip.c_str(), port);
  if (retVal == NULL || retVal -> err) {
    if (retVal) {
      cerr << "Error: " << retVal -> errstr << endl;
      redisFree(retVal);
    } else {
      cerr << "redis context creation error" << endl;
    }
    throw REDIS_CREATION_FAILURE;
  }
  return retVal;
}

int RedisUtil::blpopContent(redisContext* rContext, const char* key, char* dst, int length) {
  redisReply* rReply = (redisReply*)redisCommand(rContext, "blpop %s 0", key);

  if (rReply -> type != REDIS_REPLY_STRING) {
    // failure
    freeReplyObject(rReply);
    throw REDIS_BLPOP_FAILURE;
    return -1;
  }

  /**
   * if the fetched length is longer than that of the buffer length, return the
   * first length bytes
   */
  length = min((int)rReply -> len, length);
  memcpy(dst, rReply -> str, length);
  freeReplyObject(rReply);
  return length;
}

void RedisUtil::rpushContent(redisContext* rContext, const char* key, const char* src, int length) {
  redisReply* rReply = (redisReply*)redisCommand(rContext, "rpush %s %b", key, src, length);

  /**
   * Normally, the return value should be an integer indicating the length of
   * list after inserting
   */
  if (rReply -> type == REDIS_REPLY_ERROR) {
    // failure
    freeReplyObject(rReply);
    throw REDIS_RPUSH_FAILURE;
    return;
  }
  freeReplyObject(rReply);
}

string RedisUtil::ip2Str(unsigned int ip) {
  struct in_addr addr = {ip};
  return string(inet_ntoa(addr));
}
