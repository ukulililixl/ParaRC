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
};


#endif
