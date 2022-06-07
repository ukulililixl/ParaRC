#ifndef _SOLUTION_HH_
#define _SOLUTION_HH_

#include "../inc/include.hh"
#include "../util/DistUtil.hh"

using namespace std;

class Solution {
    private:
        Solution* _prev;
        Solution* _next;
        bool _expanded;
        int _type; // 0: head; 1: tail; 2: node

        // the number of vertices
        int _v;
        // the number of colors
        int _m;
        // current coloring
        vector<int> _solution;

        int _bdwt;
        int _load;

    public:
        Solution(bool type);
        // generate a random solution
        Solution(int v, int m);
        Solution(int v, int m, vector<int> sol);
        ~Solution();

        void setNext(Solution* sol);
        Solution* getNext();
        void setPrev(Solution* sol);
        Solution* getPrev();
        string getString();
        void setExpanded(bool flag);
        bool getExpanded();
        vector<int> getSolution();
        void setBdwt(int bdwt);
        int getBdwt();
        void setLoad(int load);
        int getLoad();
        int getDigits();

        bool isHead();
        bool isTail();
};

#endif 
