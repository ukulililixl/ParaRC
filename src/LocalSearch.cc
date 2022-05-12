#include "common/Config.hh"
#include "common/StripeMeta.hh"
#include "ec/ECDAG.hh"
#include "common/StripeStore.hh"
#include "protocol/AGCommand.hh"
#include "util/BlockingQueue.hh"
#include "util/DistUtil.hh"

#include "ec/Clay.hh"
#include "ec/IA.hh"
#include "ec/BUTTERFLY64.hh"

#include "dist/Solution.hh"

using namespace std;

void usage() {
  cout << "Usage: ./LocalSearch" << endl;
  cout << "    1. code" << endl;
  cout << "    2. n" << endl;
  cout << "    3. k" << endl;
  cout << "    4. w" << endl;
  cout << "    5. repairIdx" << endl;
  cout << "    6. round" << endl;
}  

void stat(unordered_map<int, int> sidx2ip, vector<int> curres, vector<int> itm_idx, ECDAG* ecdag, int* bdwt, int* maxload) {

    unordered_map<int, int> coloring_res;
    for (auto item: sidx2ip) {
      coloring_res.insert(make_pair(item.first, item.second));
    }
    for (int ii=0; ii<curres.size(); ii++) {
      int idx = itm_idx[ii];
      int color = curres[ii];
      coloring_res[idx] = color;
    }

    //cout << "current coloring:  " << endl;
    //for (auto item: coloring_res) {
    //  cout << "  " << item.first << ", " << item.second << endl;
    //}

    //// specific for IA
    //cout << "coloring (0,1,2): " << coloring_res[0] << " " << coloring_res[1] << " " << coloring_res[2] << endl;

    // gen ECClusters
    ecdag->clearECCluster();
    ecdag->genECCluster(coloring_res);
    
    // gen stat
    unordered_map<int, int> inmap;
    unordered_map<int, int> outmap;
    ecdag->genStat(coloring_res, inmap, outmap);

    //cout << "inmap: " << endl;
    //for (auto item: inmap) {
    //  cout << "  " << item.first << ": " << item.second << endl;
    //}

    //cout << "outmap: " << endl;
    //for (auto item: outmap) {
    //  cout << "  " << item.first << ": " << item.second << endl;
    //}

    int bw=0, max=0;
    for (auto item: inmap) {
      bw+= item.second;
      if (item.second > max)
        max = item.second;
    }
    for (auto item: outmap) {
      if (item.second > max)
        max = item.second;
    }

    *bdwt = bw;
    *maxload = max;
}

double percentage(double found, double total) {  
    return (double)found/(double)total;
}

void get_line(int x1, int y1, int x2, int y2, double* a, double* c) {
    *c = (double)(y1*x2-y2*x1)/(double)(x2-x1);
    *a = (double)(*c-y1)/(double)x1;
}

void expand(BlockingQueue<Solution*>* queue, int targeth, double C, int MAXLOAD, 
        unordered_map<string, bool>& visited, unordered_map<int, Solution*>& horizon,
        unordered_map<int, int> sidx2ip, vector<int> itm_idx, ECDAG* ecdag,
        int v, int m, double a, int digits) {
    // before a solution is added to a queue, we are sure that
    int qsize = queue->getSize();

    while (queue->getSize()) {
        Solution* cur_solution = queue->pop();
        // cout << "pop " << cur_solution->getString() << endl;
        
        // 1. calculate stat
        int cur_bdwt, cur_maxload;
        stat(sidx2ip, cur_solution->getSolution(), itm_idx, ecdag, &cur_bdwt, &cur_maxload);
        int cur_h = cur_solution->getH(cur_maxload, cur_bdwt);
        // cout << "  h: " << cur_h << endl;

        // 2. check whether to expand this solution
        if (cur_h > targeth) {
            // we are not interested in this solution, give up expanding it
            // cout << "  cur_h " << cur_h << " is larger than targeth " << targeth << ", drop it" << endl;
            delete cur_solution;
            continue;
        }

        // 3. update horizon if possible
        bool inserted = false;
        if (horizon.find(cur_maxload) == horizon.end()) {
            if (cur_maxload <= MAXLOAD) {
                horizon.insert(make_pair(cur_maxload, cur_solution));
                inserted = true;
                // cout << "  update horizon[" << cur_maxload << "] with " << cur_solution->getString() << ", load: " << cur_solution->getMaxLoad() << ", bdwt: " << cur_solution->getBdwt() << endl;
            }
        } else {
            if (cur_bdwt < horizon[cur_maxload]->getBdwt()) {
                inserted = true;
                // cout << "  old horizon[" << cur_maxload << "] :" 
                //     << horizon[cur_maxload]->getString() 
                //     << ", load: " << horizon[cur_maxload]->getMaxLoad() 
                //     << ", bdwt: " << horizon[cur_maxload]->getBdwt() << endl;

                delete horizon[cur_maxload];
                horizon[cur_maxload] = cur_solution;
                // cout << "  new horizon[" << cur_maxload << "] :" 
                //     << horizon[cur_maxload]->getString() 
                //     << ", load: " << horizon[cur_maxload]->getMaxLoad() 
                //     << ", bdwt: " << horizon[cur_maxload]->getBdwt() << endl;
            }
        }

        // now we have a solution that <= targeth
        // 4. get all the expansions of the current solution
        vector<int> cur_coloring = cur_solution->getSolution();
        // cout << "  expand: " << endl;
        for (int i=0; i<v; i++) {
            int oldv = cur_coloring[i];
            for (int j=0; j<m; j++) {
                if (j == oldv)
                    continue;
                // update the coloring of one vertex
                cur_coloring[i] = j;
                // check whether this solution has been visited
                string tmps = DistUtil::vec2str(cur_coloring, digits);
                if (visited.find(tmps) != visited.end()) {
                    // this solution has been visited before, giveup
                    // cout << "    " << tmps << " has been visited" << endl;
                } else {
                    // this solution hasn't been visited before, generate a new
                    // solution and add to queue
                    Solution* next = new Solution(v, m, a, cur_coloring);
                    queue->push(next);
                    visited.insert(make_pair(tmps, 1));
                    // cout << "    add " << tmps << " to queue for further validating" << endl;
                }
                // revert
                cur_coloring[i] = oldv;
            }
        }

        // 5. update targeth if possible
        if (cur_h < targeth && cur_h >= C) {
            targeth = cur_h;
            // cout << "  targeth: " << targeth << endl;
        }

        if (!inserted)
            delete cur_solution;
    }
}

unordered_map<int, Solution*> LocalSearch(int rounds, vector<int> itm_idx, vector<int> candidates, double a, 
        unordered_map<int, int> sidx2ip, ECDAG* ecdag, double C, int MAXLOAD,
        unordered_map<string, bool>& VISITED) {
    // in each round, we randomly select a solution and expand from this solution
    // we add a solution to visited once we have detected it, no matter whether
    // it matches our goal.
    //unordered_map<string, bool> visited;
    // always keep the min h Solution in horizon for each bdwt
    unordered_map<int, Solution*> horizon;
    // on push: if the item hasn't been processed before
    // on pop: 
    //   calculate the stat for the current solution
    //   update horizon if possible
    //   if h is less or equal to our target, we expand this solution. If a combination
    //       in the exansion hasn't been visited, we add it to the queue; update
    //       target if it is larger than H, otherwise, do not change.
    //   if h is larger than our target, we drop it.
    BlockingQueue<Solution*>* queue = new BlockingQueue<Solution*>();
    for (int i=0; i<rounds; i++) {
        // randomly select a solution
        Solution* init_solution;
        unordered_map<string, bool> visited;
        while (true) {
            init_solution = new Solution(itm_idx.size(), candidates.size(), a, true);
            string tmps = init_solution->getString();

            // we have find a solution that haven't been visited before
            if (visited.find(tmps) == visited.end()) {
                queue->push(init_solution);
                // mark it as visited
                visited.insert(make_pair(tmps, 1));
                break;
            } else
                delete init_solution;

        }

        // get h for the initialized solution
        int init_bdwt, init_maxload;
        stat(sidx2ip, init_solution->getSolution(), itm_idx, ecdag, &init_bdwt, &init_maxload);
        int h = init_solution->getH(init_maxload, init_bdwt);
        int v = init_solution->getV();
        int m = init_solution->getM();
        int digits = DistUtil::ndigits(m);
        // now we try to expand
        // cout << "round: " << i << ", init: " << init_solution->getString() << ", maxload: " << init_maxload << ", bdwt: " << init_bdwt << ", h: " << C << endl;
        expand(queue, h, C, MAXLOAD, visited, horizon, sidx2ip, itm_idx, ecdag, v, m, a, digits);

        // merge visited to VISITED
        for (auto item: visited) {
            if (VISITED.find(item.first) == VISITED.end())
                VISITED.insert(make_pair(item.first, item.second));
        }
    }
    // after we finish all the rounds, local search ends
    // return the horizon we get
    return horizon;
}

int main(int argc, char** argv) {

  if (argc != 7) {
    usage();
    return 0;
  }

  string code = argv[1];
  int n = atoi(argv[2]);
  int k = atoi(argv[3]);
  int w = atoi(argv[4]);
  int repairIdx = atoi(argv[5]);
  int rounds = atoi(argv[6]);

  // 0. calculate the repairbdwt and maxload for repair pipelining
  int rp_bdwt = k*w;
  int rp_maxload = w;

  // 1. calculate the repairbdwt and maxload for regenerating code
  int naive_bdwt = w * (n-1)/(n-k);
  int naive_maxload = naive_bdwt;

  // 2. calculate ax+y=C
  double a, C;
  get_line(rp_maxload, rp_bdwt, naive_maxload, naive_bdwt, &a, &C);
  cout << "a: " << a << ", C: " << C << endl;

  // 3. construct decode ECDAG
  ECBase* ec;
  vector<string> param;
  if (code == "Clay")
    ec = new Clay(n, k, w, {to_string(n-1)});
  else if (code == "IA")
    ec = new IA(n, k, w, param);
  else if (code == "Butterfly")
    ec = new BUTTERFLY64(n, k, w, param);
  else {
    cout << "wrong ec id!" << endl;
    return -1;
  }
  w = ec->_w;

  vector<int> avail;
  vector<int> torepair;
  for (int i=0; i<n; i++) {
    for (int j=0; j<w; j++) {
      int idx = i*w+j;
      if (i == repairIdx)
        torepair.push_back(idx);
      else
        avail.push_back(idx);
    }
  }

  ECDAG* ecdag = ec->Decode(avail, torepair);
  ecdag->Concact(torepair);
//  ecdag->dump();

  // divide ecdag into ecunits
  ecdag->genECUnits();

  // get data structures from ecdag
  unordered_map<int, ECNode*> ecNodeMap = ecdag->getECNodeMap();
  vector<int> ecHeaders = ecdag->getECHeaders(); 
  vector<int> ecLeaves = ecdag->getECLeaves();
  unordered_map<int, ECUnit*> ecunits = ecdag->getUnitMap();
  vector<int> ecUnitList = ecdag->getUnitList();

  cout << "Total nodes: " << ecNodeMap.size() << endl;
  cout << "Header nodes: " << ecHeaders.size() << endl;
  cout << "Leaf nodes: " << ecLeaves.size() << endl;
  int intermediate_num = ecNodeMap.size() - ecHeaders.size() - ecLeaves.size();
  cout << "Intermediate nodes: " << intermediate_num << endl;

  // suppose the number of available nodes equals to n
  // idx from 0, 1, ..., n
  // we first color the leave nodes and header nodes
  unordered_map<int, int> sidx2ip;
  for (auto sidx: ecLeaves) {
    int bidx = sidx / w;
    if (bidx < n)
        sidx2ip.insert(make_pair(sidx, bidx));
  }
  // figure out header color
  int bidx = torepair[0]/w;
  for (auto sidx: ecHeaders) {
    sidx2ip.insert(make_pair(sidx, bidx));
  }

  // now we try to color the intermediate node
  vector<int> itm_idx;
  vector<int> candidates;
  for (auto item: ecNodeMap) {
    int sidx = item.first;
    if (find(ecHeaders.begin(), ecHeaders.end(), sidx) != ecHeaders.end())
      continue;
    if (find(ecLeaves.begin(), ecLeaves.end(), sidx) != ecLeaves.end())
      continue;
    itm_idx.push_back(sidx);
    sidx2ip.insert(make_pair(sidx, -1));
  }
  for (int i=0; i<n; i++)
    candidates.push_back(i);
  sort(itm_idx.begin(), itm_idx.end());

  //cout << "itm_idx: ";
  //for (int i=0; i<itm_idx.size(); i++)
  //  cout << itm_idx[i] << " ";
  //cout << endl;

  // 4. start searching in multiple rounds

  unordered_map<string, bool> visited;
  unordered_map<int, Solution*> horizon = LocalSearch(
          rounds, itm_idx, candidates, a, sidx2ip, ecdag, C, naive_maxload, visited);

  cout << "The number of solutions visited: " << visited.size() << endl;
  cout << "The number of solutions in horizon: " << horizon.size() << endl;

  for(auto item: horizon) {
      Solution* s = item.second;
      cout << "Solution " << s->getString() << ", maxload: " << s->getMaxLoad() << ", bdwt: " << s->getBdwt() << endl;
  }

  return 0;
}
