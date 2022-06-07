#include "common/Config.hh"
#include "common/StripeMeta.hh"
#include "ec/ECDAG.hh"
#include "common/StripeStore.hh"
#include "protocol/AGCommand.hh"
#include "util/DistUtil.hh"

#include "ec/Clay.hh"
#include "ec/IA.hh"
#include "ec/BUTTERFLY64.hh"

#include "dist/Solution.hh"

using namespace std;

void usage() {
  cout << "Usage: ./TradeoffAnalysis" << endl;
  cout << "    1. code" << endl;
  cout << "    2. n" << endl;
  cout << "    3. k" << endl;
  cout << "    4. w" << endl;
  cout << "    5. repairIdx" << endl;
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

bool dominate(Solution* A, Solution* B) {
    // returns true if A dominates B
    int loada = A->getLoad();
    int bdwta = A->getBdwt();

    int loadb = B->getLoad();
    int bdwtb = B->getBdwt();

    if (loada < loadb) {
        if (bdwta < bdwtb) {
            return true;
        } else if (bdwta == bdwtb) {
            return true;
        } else {
            return false;
        }
    } else if (loada > loadb) {
        if (bdwta < bdwtb) {
            return false;
        } else if (bdwta == bdwtb) {
            return false;
        } else {
            return false;
        }
    } else if (loada == loadb) {
        if (bdwta < bdwtb) {
            return true;
        } else if (bdwta == bdwtb) {
            return false;
        } else {
            return false;
        }
    }

}

bool updateTradeoffCurve(Solution* head, Solution* tail, Solution* sol, 
        unordered_map<int, int>& load2bdwt, unordered_map<int, int>& bdwt2load) {

    bool status = false;
    int sol_load = sol->getLoad();
    int sol_bdwt = sol->getBdwt();

    Solution* current = head;

    current = head;
    //cout << "before:--------------------";
    //while(current) {
    //    cout << current->getString() << "; ";
    //    current = current->getNext();
    //}
    //cout << endl;

    current = head;
    Solution* prev = NULL;
    Solution* next = NULL;
    vector<Solution*> toremove;
    //cout << "          check tradeoff curve for " << sol->getString() << endl;

    while (current->getNext() != tail) {
        current = current->getNext();
        if (current == sol) {
            //cout << "            current == sol == " << sol->getString() << ", continue;" << endl;
            continue;
        } else if (current->getString() == sol->getString()) {
            //cout << "            current == sol == " << sol->getString() << ", break;" << endl;
            break;
        }

        int cur_load = current->getLoad();
        int cur_bdwt = current->getBdwt();
        //cout << "            cmp with: " << current->getString() << ", load: " << current->getLoad() << ", bdwt: " << current->getBdwt() 
        //    << ", prev: " << current->getPrev()->getString() << ", next: " << current->getNext()->getString() << endl;

        // 1. current dominates sol
        if (dominate(current, sol)) {
            // we cannot update 
            //cout << "              current dominate sol" << endl;
            break;
        }

        // 2. sol dominates current
        if (dominate(sol, current)) {
            // update prev if this is the first point we find that is dominated
            // by sol
            //cout << "              sol dominate current, current->prev: " << current->getPrev()->getString() << ", current->next: " << current->getNext()->getString() << endl;
            if (prev == NULL) {
                prev = current->getPrev();
                prev->setNext(sol);
                sol->setPrev(prev);
                current->setPrev(NULL);
                //cout << "                update sol->prev with current->prev, sol->prev: " << sol->getPrev()->getString() << endl;
            }

            // update next
            next = current->getNext();
            //cout << "                current->next: " << next->getString() << endl;
            sol->setNext(next);
            next->setPrev(sol);
            //cout << "                update sol->next with current->next, sol->next: " << sol->getNext()->getString() << endl;

            load2bdwt.erase(cur_load);
            bdwt2load.erase(cur_bdwt);

            load2bdwt.insert(make_pair(sol_load, sol_bdwt));
            bdwt2load.insert(make_pair(sol_bdwt, sol_load));

            toremove.push_back(current);
            status = true;
        }

        // 3. current and sol cannot dominate each other
        if (!dominate(sol, current) && !dominate(current, sol)) {
            status = true;
            // we need to figure out to add sol before current or behind
            //cout << "              sol and current cannot dominate each other" << endl;
            if (sol_load < cur_load) {
                if (prev == NULL) {
                    // add sol before current, and sol hasn't been inserted
                    //cout << "                insert sol before current" << endl;
                    prev = current->getPrev();
                    prev->setNext(sol);
                    sol->setPrev(prev);
                    sol->setNext(current);
                    current->setPrev(sol);

                    load2bdwt.insert(make_pair(sol_load, sol_bdwt));
                    bdwt2load.insert(make_pair(sol_bdwt, sol_load));
                }
            } else {
                // add sol after current
                next = current->getNext();
                if (next == tail) {
                    current->setNext(sol);
                    sol->setPrev(current);
                    sol->setNext(next);
                    next->setPrev(sol);
                    load2bdwt.insert(make_pair(sol_load, sol_bdwt));
                    bdwt2load.insert(make_pair(sol_bdwt, sol_load));
                    //cout << "                insert sol at the tail, sol->prev: " << sol->getPrev()->getString() << ", sol: " << sol->getString() << ", sol->next: " << sol->getNext()->getString() << endl;
                    //cout << "                now, current->prev: " << current->getPrev()->getString() << ", current: " << current->getString() << ", current->next: " << current->getNext()->getString() << endl;
                }
            }
        }
    }

    for (auto item: toremove)
        delete item;

    // debug
    current = head;
    //cout << "after:--------------------";
    //while(current) {
    //    cout << current->getString() << "; ";
    //    current = current->getNext();
    //}
    //cout << endl;

    return status;
}

void expand(Solution* current, int v, int m, unordered_map<string, bool>& visited,
        unordered_map<int, int> sidx2ip, vector<int> itm_idx, ECDAG* ecdag,
        unordered_map<int, int>& load2bdwt, unordered_map<int, int>& bdwt2load,
        Solution* tradeoff_curve_head, Solution* tradeoff_curve_tail) {
    cout << "    expand: " << current->getString() << ", load: " << current->getLoad() << ", bdwt: " << current->getBdwt() << endl;

    vector<int> solution = current->getSolution();
    current->setExpanded(true);

    for (int i=0; i<v; i++) {
        int oldv = solution[i];
        for (int j=0; j<m; j++) {
            if (j == oldv)
                continue;
            // update the color of one vertex
            solution[i] = j;
            Solution* neighbor = new Solution(v, m, solution);
            string tmps = neighbor->getString();
            //cout << "      neighbor: " << tmps << endl;
            
            Solution* current = tradeoff_curve_head;

            // check whether the neighbor has been visited
            if (visited.find(tmps) != visited.end()) {
                // this solution has been visited
                //cout << "        visited, skip" << endl;
                delete neighbor;
            } else {
                // this solution hasn't been visited before
                // get stat for the neighbor
                int neighbor_bdwt, neighbor_load;
                stat(sidx2ip, neighbor->getSolution(), itm_idx, ecdag, &neighbor_bdwt, &neighbor_load);
                neighbor->setBdwt(neighbor_bdwt);
                neighbor->setLoad(neighbor_load);
                visited.insert(make_pair(tmps, true));
                //cout << "        load: " << neighbor->getLoad() << ", bdwt: " << neighbor->getBdwt() << endl;

                // now we check whether the current neighbor can update
                // the tradeoff_curve. we first do a quick search
                if (load2bdwt.find(neighbor_load) != load2bdwt.end()) {
                    // a point of the same load exists in the tradeoff curve
                    if (load2bdwt[neighbor_load] <= neighbor_bdwt) {
                        // neighbor cannot update the tradeoff_curve
                        //cout << "          curve bdwt: " << load2bdwt[neighbor_load] << " is better, skip the neighbor" << endl;
                        delete neighbor;
                        neighbor = NULL;
                    }
                } else if (bdwt2load.find(neighbor_bdwt) != bdwt2load.end()) {
                    // a point of the same bdwt exists in the tradeoff curve
                    if (bdwt2load[neighbor_bdwt] <= neighbor_load) {
                        // neighbor cannot update the tradeoff curve
                        //cout << "          curve load: " << bdwt2load[neighbor_bdwt] << " is better, skip the neighbor" << endl;
                        delete neighbor;
                        neighbor = NULL;
                    }
                } 
                
                // neighbor_bdwt or neighbor_load is new
                // we iterate the tradeoff curve to insert the neighbor and
                // update two maps
                if (neighbor) {
                    if(!updateTradeoffCurve(tradeoff_curve_head, tradeoff_curve_tail, neighbor, load2bdwt, bdwt2load)) {
                        delete neighbor;
                        neighbor = NULL;
                    } else {
                        cout << "update tradeoff curve at i: " << i << ", j: " << j << endl;
                    }
                }

                current = tradeoff_curve_head;
            }
        }
        solution[i] = oldv;
    }
}

Solution* genTradeoffCurve(vector<int> itm_idx, vector<int> candidates,
        unordered_map<int, int> sidx2ip, ECDAG* ecdag) {
    // 0. initialize a head and tail with NULL for the tradeoff curve
    Solution* head = new Solution(true);
    Solution* tail = new Solution(false);

    head->setNext(tail);
    tail->setPrev(head);

    // 1. randomly select a solution and insert it into the tradeoff curve
    Solution* init_sol = new Solution(itm_idx.size(), candidates.size());
    head->setNext(init_sol);
    init_sol->setPrev(head);
    init_sol->setNext(tail);
    tail->setPrev(init_sol);

    // 1.1 get stat for the init_sol
    int v = itm_idx.size();
    int m = candidates.size();
    int init_bdwt, init_load;
    stat(sidx2ip, init_sol->getSolution(), itm_idx, ecdag, &init_bdwt, &init_load);
    init_sol->setBdwt(init_bdwt);
    init_sol->setLoad(init_load);
    cout << "genTradeoffCurve: init_sol: " << init_sol->getString() << ", load: " << init_sol->getLoad() << ", bdwt: " << init_sol->getBdwt() << endl;

    // 2. generate a map that records the solution that we visited.
    unordered_map<string, bool> visited;
    visited.insert(make_pair(init_sol->getString(), true));

    // 3. generate load2bdwt and bdwt2load map
    unordered_map<int, int> load2bdwt;
    unordered_map<int, int> bdwt2load;
    load2bdwt.insert(make_pair(init_load, init_bdwt));
    bdwt2load.insert(make_pair(init_bdwt, init_load));

    // 4. each time we choose the first solution in the tradeoff curve that
    // hasn't been expanded
    Solution* current;
    while (true) {
        current = head->getNext();

        // find the first solution that not expanded
        while (current != tail) {
            cout << "  iterate: " << current->getString() << ", load: " << current->getLoad() << ", bdwt: " << current->getBdwt() << endl;
            if (current->getExpanded()) {
                cout << "    expanded, skip" << endl;
                current = current->getNext();
            } else {
                cout << "    not expanded" << endl;
                break;
            }
        }

        if (current == tail) {
            // all the solutions has been expanded
            cout << "  reach the tail" << endl;
            break;
        }

        // now we expand the current solution
        expand(current, v, m, visited, sidx2ip, itm_idx, ecdag, load2bdwt, bdwt2load, head, tail);

    }

    //cout << "Current tradeoff curve: ";
    //current = head;
    //while(current) {
    //    cout << current->getString() << "; ";
    //    current = current->getNext();
    //}
    //cout << endl;

    return head;
}

Solution* getMLP(vector<int> itm_idx, vector<int> candidates,
        unordered_map<int, int> sidx2ip, ECDAG* ecdag, int rounds, int target_load, int target_bdwt) {
    
    Solution* prev_sol;
    Solution* cur_sol;
    bool non_update_flag = false;
    int non_update_num = 0;
    int total_num = 0;
    double non_update_ratio = 0;

    Solution* mlp_sol = NULL;
    int mlp_load = -1;
    int mlp_bdwt = -1;

    while (true) {
        Solution* head = genTradeoffCurve(itm_idx, candidates, sidx2ip, ecdag);
        
        // get the mlp 
        cur_sol = head->getNext();

        bool update = false;

        // compare mlp with mlp_sol
        if (mlp_sol == NULL) {
            mlp_sol = cur_sol;
            mlp_load = mlp_sol->getLoad();
            mlp_bdwt = mlp_sol->getBdwt();
            update = true;
        } else {
            int cur_load = cur_sol->getLoad();
            int cur_bdwt = cur_sol->getBdwt();

            if (cur_load < mlp_load) {
                // current is better than mlp
                mlp_sol = cur_sol;
                mlp_load = cur_load;
                mlp_bdwt = cur_bdwt;
                update = true;
            } else if (cur_load == mlp_load) {
                if (cur_bdwt < mlp_bdwt) {
                    // update mlp
                    mlp_sol = cur_sol;
                    mlp_load = cur_load;
                    mlp_bdwt = cur_bdwt;
                    update = true;
                }
            }
        }

        // record the number of non-update rounds in sequence
        if (update) {
            non_update_num = 0;
        } else {
            non_update_num++;
        }
        total_num++;

        // enable non_update_flag
        if (total_num < rounds) {
            if (mlp_load <= target_load && mlp_bdwt <= target_bdwt) {
                // we find a solution that is better enough, start to stat the
                // number of non-update rounds
                if (!non_update_flag) {
                    non_update_num = 0;
                    non_update_flag = true;
                    cout << "enable non_update_flag at round "  << total_num << endl;
                }
            }
        } else {
            if (!non_update_flag) {
                non_update_num = 0;
                non_update_flag = true;
                cout << "enable non_update_flag at round "  << total_num << endl;
            }
        }

        // free
        Solution* cur = cur_sol->getNext();
        Solution* next;
        while(cur) {
            next = cur->getNext();
            delete cur;
            cur = next;
        }

        if (!update)
            delete cur_sol;

        delete head;

        // calculate non_update_ratio
        non_update_ratio = (double)non_update_num / (double)total_num;
        cout << "round " << total_num-1  << ", mlp: " << mlp_sol->getString() << ", load: " << mlp_sol->getLoad() << ", bdwt: " << mlp_sol->getBdwt() 
            << ", total_num: " << total_num << ", non_update_num: " << non_update_num << ", ratio: " << non_update_ratio << endl;

        // stop?
        if (non_update_flag) {
            if (non_update_ratio > 0.1) {
                cout << "non_update_num: " << non_update_num << ", total_num: " << total_num << ", non_update_ratio: " << non_update_ratio << ", stop!" << endl;
                break;
            }
        }
    }
    return mlp_sol;
}

int main(int argc, char** argv) {

  if (argc != 6) {
    usage();
    return 0;
  }

  string code = argv[1];
  int n = atoi(argv[2]);
  int k = atoi(argv[3]);
  int w = atoi(argv[4]);
  int repairIdx = atoi(argv[5]);

  // XL: do we need number of available nodes?

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
  //ecdag->dump();

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

  // for (auto item: ecLeaves) {
  //     cout << item << ", ";
  // }
  // cout << endl;

  int intermediate_num = ecNodeMap.size() - ecHeaders.size() - ecLeaves.size();
  cout << "Intermediate nodes: " << intermediate_num << endl;

  // suppose the number of available nodes equals to n
  // idx from 0, 1, ..., n
  // we first color the leave nodes and header nodes
  unordered_map<int, int> sidx2ip;
  for (auto sidx: ecLeaves) {
    int bidx = sidx / w;
    if (bidx < n )
        sidx2ip.insert(make_pair(sidx, bidx));
    else
        sidx2ip.insert(make_pair(sidx, -1));
  }

  // // debug
  // for (auto item: sidx2ip) {
  //     cout << item.first << ": " << item.second << endl;
  // }

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

  // cout << "before coloring the intermediate vertex: " << endl;
  // for (auto item: sidx2ip) {
  //   cout << "  " << item.first << ": " << item.second << endl;
  // }

  // cout << "intermediate vertex: ";
  // for (auto idx: itm_idx)
  //   cout << idx <<  " ";
  // cout << endl;

  // cout << "candidates: ";
  // for (auto color: candidates) {
  //   cout << color << " ";
  // }
  // cout << endl;
  
  // The size of the solution space
  double spacesize = pow(candidates.size(), itm_idx.size());
  cout << "Spacesize: " << spacesize << endl;

  // simple search
  
  vector<int> curres;
  for (int i=0; i<itm_idx.size(); i++)
    curres.push_back(-1);

  double found=0;
  unordered_map<int, vector<int>> max2bwlist;
  unordered_map<int, double> process;

  int round = k*w;
  if (code == "Clay" && n==14)
      round = 1;

  struct timeval time1, time2;
  gettimeofday(&time1, NULL);
  Solution* mlp = getMLP(itm_idx, candidates, sidx2ip, ecdag, n*w, w, k*w);
  gettimeofday(&time2, NULL);
  double latency = DistUtil::duration(time1, time2);
  cout << "Runtime: " << latency << endl;

  // print the mlp
  cout << "Digits: " << mlp->getDigits() << ", string: " << mlp->getString() << endl;

  return 0;
}
