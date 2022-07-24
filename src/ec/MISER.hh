#ifndef _MISER_HH_
#define _MISER_HH_

#include "Computation.hh"
#include "ECBase.hh"
//#include "ECDAG.hh"

#define MISER_MAX 512

using namespace std;

class MISER : public ECBase {
  private:
    int _chunk_num_per_node;
    int _sys_chunk_num;
    int _enc_chunk_num;
    int _total_chunk_num;

    int _ori_encoding_matrix[MISER_MAX*MISER_MAX];
    int _dual_enc_matrix[MISER_MAX*MISER_MAX];
    int _offline_enc_vec[MISER_MAX*MISER_MAX];
    int _final_enc_matrix[MISER_MAX*MISER_MAX];
    int _recovery_equations[MISER_MAX*MISER_MAX];

    void generate_encoding_matrix();
    void generate_decoding_matrix(int rBlkIdx);
    void square_cauchy_matrix(int *des, int size);
  public:
    MISER(int n, int k, int w, vector<string> param);
    //MISER(int n, int k, int w, int opt, vector<string> param);
    ECDAG* Encode();
    ECDAG* Decode(vector<int> from, vector<int> to);
    //void Place(vector<vector<int>>& group);
};


#endif
