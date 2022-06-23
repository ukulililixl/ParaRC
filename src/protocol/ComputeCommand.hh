#ifndef _COMPUTECOMMAND_HH_
#define _COMPUTECOMMAND_HH_

#include "../inc/include.hh"
#include "../util/RedisUtil.hh"

using namespace std;

class ComputeCommand {
    private:
        vector<int> _srclist;
        vector<int> _dstlist;
        vector<vector<int>> _coefs;

        string _stripeName;
        int _taskid;

        char* _computeCmd;
        int _cmLen;
        string _rKey;

    public:
        ComputeCommand(string stripename,
                vector<int> srclist,
                vector<int> dstlist,
                vector<vector<int>> coefs,
                int taskid);
        ComputeCommand(char* reqStr);
        ~ComputeCommand();

        void writeInt(int value);
        void writeString(string s);
        int readInt();
        string readString();

        vector<int> getSrcList();
        vector<int> getDstList();
        vector<vector<int>> getCoefs();
        void setCmd(char* cmd, int len);

        void sendTo(unsigned int ip);
        void buildCommand();
};

#endif
