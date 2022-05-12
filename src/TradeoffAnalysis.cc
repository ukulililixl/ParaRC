#include "common/Config.hh"
#include "common/StripeMeta.hh"
#include "ec/ECDAG.hh"
#include "common/StripeStore.hh"
#include "protocol/AGCommand.hh"

#include "ec/Clay.hh"
#include "ec/IA.hh"
#include "ec/BUTTERFLY64.hh"

using namespace std;

void usage() {
  cout << "Usage: ./TradeoffAnalysis" << endl;
  cout << "    1. code" << endl;
  cout << "    2. n" << endl;
  cout << "    3. k" << endl;
  cout << "    4. w" << endl;
  cout << "    5. repairIdx" << endl;
}  

double getCurrentTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (double)tv.tv_sec * 1e+6 + (double)tv.tv_usec;
}

void enumerate_combinations(vector<vector<int>>& res, vector<int>& curres, int curidx, int nodenum, vector<int> candidates) {
//  for (int i=0; i<nodenum; i++) {
//    cout << curres[i] << " ";
//  }
//  cout << endl;

  if (curidx == nodenum) {
    // we have find a solution
    res.push_back(curres);
    return;
  }

  for (int i=0; i<candidates.size(); i++) {
    int curcolor = candidates[i];

    // try to color curidx to curcolor
    curres[curidx] = curcolor;

    // try the next idx;
    enumerate_combinations(res, curres, curidx+1, nodenum, candidates);

    // try to uncolor curidx to -1;
    curres[curidx] = -1;
  }
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
//  ecdag->dump();

  // divide ecdag into ecunits
  ecdag->genECUnits();

  // get data structures from ecdag
  unordered_map<int, ECNode*> ecNodeMap = ecdag->getECNodeMap();
  vector<int> ecHeaders = ecdag->getECHeaders(); 
  vector<int> ecLeaves = ecdag->getECLeaves();
  unordered_map<int, ECUnit*> ecunits = ecdag->getUnitMap();
  vector<int> ecUnitList = ecdag->getUnitList();

  //cout << "Total nodes: " << ecNodeMap.size() << endl;
  //cout << "Header nodes: " << ecHeaders.size() << endl;
  //cout << "Leaf nodes: " << ecLeaves.size() << endl;
  int intermediate_num = ecNodeMap.size() - ecHeaders.size() - ecLeaves.size();
  //cout << "Intermediate nodes: " << intermediate_num << endl;

  // suppose the number of available nodes equals to n
  // idx from 0, 1, ..., n
  // we first color the leave nodes and header nodes
  unordered_map<int, int> sidx2ip;
  for (auto sidx: ecLeaves) {
    int bidx = sidx / w;
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

  // we first enumerate all possible combinations 
  vector<vector<int>> enumres;
  vector<int> curres;
  for (int i=0; i<itm_idx.size(); i++)
    curres.push_back(-1);

  enumerate_combinations(enumres, curres, 0, itm_idx.size(), candidates); 
  cout << "the number of combinations: " << enumres.size() << endl;

  //for (int i=0; i<enumres.size(); i++) {
  //  cout << "res[" << i << "]: ";
  //  vector<int> tmpres = enumres[i];
  //  for (int ii=0; ii<tmpres.size(); ii++)
  //    cout << tmpres[ii] << " ";
  //  cout << endl;
  //}

  unordered_map<int, vector<int>> max2bwlist;
  for (int i=0; i<enumres.size(); i++) {
    if (i%10000 == 0)
      cout << "solution: " << i << endl;
    // for each coloring method, merge ECUnits into ECClusters
    vector<int> curres = enumres[i];

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
    //cout << "coloridx: " << i << ", curbw: " << bw << ", curmax: " << max << endl;
    if (max2bwlist.find(max) == max2bwlist.end()) {
      vector<int> curlist = {bw};
      max2bwlist.insert(make_pair(max, curlist));
    } else {
      vector<int> curlist = max2bwlist[max];
      if (find(curlist.begin(), curlist.end(), bw) == curlist.end())
        max2bwlist[max].push_back(bw);
    }

  }
  
  for (auto item: max2bwlist) {
    int max = item.first;
    for (auto bw: max2bwlist[max])
      cout << max <<  "\t" << bw << endl;
  }
  return 0;
}
