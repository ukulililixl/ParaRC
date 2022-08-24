#ifndef _DISTUTIL_HH_
#define _DISTUTIL_HH_


#include "../inc/include.hh"

using namespace std;

class DistUtil {
  public:
    static double duration(struct timeval t1, struct timeval t2);
    static vector<string> str2container(string line);
    static vector<string> splitStr(string s, string delimiter);
    static vector<string> listFiles(string dirpath);
    static double average(vector<int> list);
    static double variance(vector<int> list, double mean);
    static int ndigits(int m);
    static string num2str(int num, int digits);
    static string vec2str(vector<int> v, int digits);
    static string execShellCmd(string command);
    static string getFullPathForBlock(string blockDir, string blockName);
};


#endif
