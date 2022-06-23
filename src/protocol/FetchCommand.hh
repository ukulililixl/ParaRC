#ifndef _FETCHCOMMAND_HH_
#define _FETCHCOMMAND_HH_

#include "../inc/include.hh"
#include "../util/RedisUtil.hh"

using namespace std;

class FetchCommand {
  private:
      char* _fetchCmd = 0;
      int _cmLen = 0;
      string _rKey;

      string _stripeName;
      unsigned int _fetchip;
      vector<int> _cidlist;
      int _taskid;

  public:
      FetchCommand(string stripename,
              unsigned int fetchip,
              vector<int> cidlist,
              int taskid);
      FetchCommand(char* reqStr);
      ~FetchCommand();

      void writeInt(int value);
      void writeString(string s);
      int readInt();
      string readString();

      unsigned int getFetchIp();
      vector<int> getCidList();
      void setCmd(char* cmd, int len);

      void sendTo(unsigned int ip);

      void buildCommand();
};

#endif
