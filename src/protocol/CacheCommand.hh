#ifndef _CACHECOMMAND_HH_
#define _CACHECOMMAND_HH_

#include "../inc/include.hh"
#include "../util/RedisUtil.hh"

using namespace std;

class CacheCommand {
    private:
        unordered_map<int, int> _cid2refs;
        string _stripeName;
        int _taskid;

        char* _cacheCmd;
        int _cmLen;
        string _rKey;

    public:
        CacheCommand(string stripename,
                unordered_map<int, int> cid2refs,
                int taskid);
        CacheCommand(char* reqStr);
        ~CacheCommand();

        void writeInt(int value);
        void writeString(string s);
        int readInt();
        string readString();

        unordered_map<int, int> getCid2Refs();
        void setCmd(char* cmd, int len);
        
        void sendTo(unsigned int ip);
        void buildCommand();
};

#endif
