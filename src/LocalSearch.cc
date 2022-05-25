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

BlockingQueue<Solution*>* getLongestQueue(unordered_map<int, BlockingQueue<Solution*>*> queuemap) {
    BlockingQueue<Solution*>* toret;
    long long len = -1;

    for (auto item: queuemap) {
        BlockingQueue<Solution*>* q = item.second;
        if (len == -1) {
            toret = q;
            len = q->getSize();
        } else {
            if (q->getSize() > len) {
                toret = q;
                len = q->getSize();
            }
        }
    }
    return toret;
}

void expand(unordered_map<int, Solution*>& horizon, unordered_map<int, BlockingQueue<Solution*>*>& queuemap,
        unordered_map<int, int>& load2bdwt, unordered_map<string, int>& visited,
        unordered_map<int, int> sidx2ip, vector<int> itm_idx, ECDAG* ecdag,
        int v, int m, double a, int digits, double naive_maxload) {
    // once a solution is generated, it is added into the queuemap,
    // corresponding load2bdwt is updated, and it is marked as visisted.
    // Thus, the init solution has been initialized in each data structure

    while (true) {
        // 1. find out the queue that has the maximum size
        BlockingQueue<Solution*>* cur_queue = getLongestQueue(queuemap);
        if (cur_queue->getSize() == 0){
            // all the solution has been processed
            break;
        }

        // 2. find the solution in which the bdwt equals to the min
        assert(cur_queue->getSize() == 1);
        Solution* cur_solution = cur_queue->pop();
        int cur_maxload = cur_solution->getMaxLoad();
        int cur_bdwt = cur_solution->getBdwt();
        int cur_queuemin = load2bdwt[cur_maxload];
        assert(cur_queuemin == cur_bdwt);
        //cout << cur_solution->getString() << " - maxload: " << cur_maxload << ", bdwt: " << cur_bdwt << ", qsize: " << cur_queue->getSize() << ", qmin: " << cur_queuemin << endl;

        // 3. compare the item with horizonbase
        Solution* horizonbase;
        bool inserted = false;
        assert(cur_maxload <= naive_maxload);
        if (horizon.find(cur_maxload) == horizon.end()) {
            horizon.insert(make_pair(cur_maxload, cur_solution));
            inserted = true;
            cout << "update horizon maxload: " << cur_maxload << ", bdwt: " << cur_bdwt << ", visit.size: " << visited.size() << endl;;
        } else {
            horizonbase = horizon[cur_maxload];
            int oldbdwt = horizonbase->getBdwt();
            if (cur_bdwt < oldbdwt) {
                delete horizonbase;
                horizon[cur_maxload] = cur_solution;
                inserted = true;
                cout << "update horizon maxload: " << cur_maxload << " from bdwt: " << oldbdwt << " to bdwt: " << cur_bdwt << ", visited.size: " << visited.size() << endl;
            }
        }

        if (!inserted) {
            delete cur_solution;
            continue;
        }

        // 4. expand the current solution
        vector<int> cur_coloring = cur_solution->getSolution();
        for (int i=0; i<v; i++) {
            int oldv = cur_coloring[i];
            for (int j=0; j<m; j++) {
                if (j == oldv)
                    continue;
                // update the coloring of one vertex
                cur_coloring[i] = j;
                Solution* next = new Solution(v, m, a, cur_coloring);
                string tmps = next->getString();

                // check whether this solution has been visited
                if (visited.find(tmps) != visited.end()) {
                    // this solution has been visited before, giveup
                    //cout << "    " << tmps << " has been visited" << endl;
                    delete next;
                } else {
                    // this solution hasn't been visited before 
                    int next_bdwt, next_maxload, next_h;
                    stat(sidx2ip, next->getSolution(), itm_idx, ecdag, &next_bdwt, &next_maxload);
                    next_h = next->getH(next_maxload, next_bdwt);
                    visited.insert(make_pair(tmps, 1));

                    // check with load2bdwt
                    if (load2bdwt.find(next_maxload) == load2bdwt.end()) {
                        // we find a new maxload
                        if (next_maxload <= naive_maxload) {
                            load2bdwt.insert(make_pair(next_maxload, next_bdwt));
                            // create new queue
                            BlockingQueue<Solution*>* next_queue = new BlockingQueue<Solution*>();
                            next_queue->push(next);
                            queuemap.insert(make_pair(next_maxload, next_queue));
                            // cout << "  update queue " << next_maxload << ", min " << next_bdwt << endl;
                        } else {
                            // maxload is too large
                            delete next;
                        }
                    } else {
                        assert(next_maxload <= naive_maxload);
                        int next_queuemin = load2bdwt[next_maxload];
                        // compare with min
                        if (next_bdwt > next_queuemin) {
                            // bdwt is too high
                            delete next;
                        } else if (next_bdwt == next_queuemin) {
                            // ignore it
                            delete next;
                        } else {
                            // update min and queuemap
                            load2bdwt[next_maxload] = next_bdwt;
                            // get the queue
                            BlockingQueue<Solution*>* next_queue = queuemap[next_maxload];
                            while (next_queue->getSize()) {
                                Solution* tmp_solution = next_queue->pop();
                                delete tmp_solution;
                            }
                            next_queue->push(next);
                            // cout << "  update queue " << next_maxload << ", min from " << next_queuemin << " to " << next_bdwt << endl;
                        }
                    }
                }
                // revert
                cur_coloring[i] = oldv;
            }
        }
    }
}

bool check_horizon(unordered_map<int, Solution*> horizon, double C) {
    if (horizon.size() == 0)
        return true;

    for (auto item: horizon) {
        Solution* cur_solution = item.second;
        if (cur_solution->getH() > C)
            return true;
    }

    return false;
}

unordered_map<int, Solution*> LocalSearch(double C, double a, double naive_maxload,
        vector<int> itm_idx, vector<int> candidates,  
        unordered_map<int, int> sidx2ip, ECDAG* ecdag) {

    unordered_map<int, Solution*> HORIZON;

    while (check_horizon(HORIZON, C)) {
        // prepare data structures for this search
        unordered_map<int, Solution*> horizon;
        unordered_map<int, BlockingQueue<Solution*>*> queuemap; // only keep 1 item in each queue
        unordered_map<int, int> load2bdwt; // maintains the minimum bdwt
        unordered_map<string, int> visited; // records whether visited

        // randomly select a solution
        Solution* init_solution = new Solution(itm_idx.size(), candidates.size(), a, true);
        int init_bdwt, init_maxload, init_h;
        string tmps;

        while (true) {
            init_solution = new Solution(itm_idx.size(), candidates.size(), a, true);
            stat(sidx2ip, init_solution->getSolution(), itm_idx, ecdag, &init_bdwt, &init_maxload);
            init_h = init_solution->getH(init_maxload, init_bdwt);
            tmps = init_solution->getString();
            visited.insert(make_pair(tmps, 1));
            if (init_maxload <= naive_maxload)
                break;
            else
                delete init_solution;
        }

        // get stat for this solution
        int v = init_solution->getV();
        int m = init_solution->getM();
        int digits = DistUtil::ndigits(m);

        // update data structures for this solution
        BlockingQueue<Solution*>* cur_queue = new BlockingQueue<Solution*>();
        cur_queue->push(init_solution);
        queuemap.insert(make_pair(init_maxload, cur_queue));
        load2bdwt.insert(make_pair(init_maxload, init_bdwt));
        
        // now we expand this solution
        expand(horizon, queuemap, load2bdwt, visited, sidx2ip, itm_idx, ecdag, v, m, a, digits, naive_maxload);
        cout << "horizon.size: " << horizon.size() << endl;

        cout << "expand ends!!!!!!!!!!!!!!!!!!!!!!!!!!1" << endl;

        // after the expansion, we check whether there are updates in horizon
        bool update = false;
        vector<Solution*> toremove;
        for (auto item: horizon) {
            int cur_maxload = item.first;
            Solution* cur_solution = item.second;
            int cur_bdwt = cur_solution->getBdwt();
            if (HORIZON.find(cur_maxload) == HORIZON.end()) {
                HORIZON.insert(make_pair(cur_maxload, cur_solution));
                update = true;
            } else {
                Solution* horizonbase = HORIZON[cur_maxload];
                if (cur_bdwt < horizonbase->getBdwt()) {
                    HORIZON[cur_maxload] = cur_solution;
                    toremove.push_back(horizonbase);
                    update = true;
                } else {
                    toremove.push_back(cur_solution);
                }
            }
        }

        for (auto item: HORIZON) {
            Solution* cur_solution = item.second;
            int cur_maxload = cur_solution->getMaxLoad();
            int cur_bdwt = cur_solution->getBdwt();
            cout << "HORIZON: cur_maxload: " << cur_maxload << ", cur_bdwt: " << cur_bdwt << endl;
        }

        // free
        for (auto item: toremove) {
            delete item;
        }
        for (auto item: queuemap) {
            delete item.second;
        }

        cout << "Sleep 15s !!!!!!!!!!!!!!!!!!!!!!!!!" << endl;
        this_thread::sleep_for(std::chrono::milliseconds(15000));
    }

    return HORIZON;
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

  unordered_map<int, Solution*> horizon = LocalSearch(
          C, a, naive_maxload, itm_idx, candidates, sidx2ip, ecdag);

  for(auto item: horizon) {
      Solution* s = item.second;
      cout << "Solution " << s->getString() << ", maxload: " << s->getMaxLoad() << ", bdwt: " << s->getBdwt() << endl;
      delete s;
  }

  delete ecdag;

  return 0;
}
