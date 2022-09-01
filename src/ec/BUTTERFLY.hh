#ifndef _BUTTERFLY_HH_
#define _BUTTERFLY_HH_

#include "Computation.hh"
#include "ECBase.hh"
#include <map>
//#include "ECDAG.hh"

#define BUTTERFLY_MAX 512
#define K_MAX 10

using namespace std;

class BUTTERFLY: public ECBase {
  private:
    int _m;
    int _r;
    int _nr;
    int _chunk_num_per_node;
    int _sys_chunk_num;
    int _enc_chunk_num;
    int _total_chunk_num;
    int _enc_matrix[BUTTERFLY_MAX * (K_MAX + 2) * BUTTERFLY_MAX * K_MAX];
    int _tmp;

    vector<vector<int>> perm_mat(vector<vector<int>> mat);
    vector<vector<int>> add_mat(vector<vector<int>> mat1, vector<vector<int>> mat2);
    vector<vector<int>> butterfly_func(vector<vector<int>> mat, int m_row, int m_col);
    void print_double_vec(vector<vector<int>> off);
    void generate_encoding_matrix();
    void generate_xor_data_matrix(int *xor_mat, int sid);
    void generate_butterfly_parity_encoding_matrix(int *parity_enc);
    void generate_butterfly_parity_decoding_matrix_1(int *parity_dec, int rBlkIdx, vector<int> off);
    void generate_butterfly_parity_decoding_matrix_2(int *parity_dec, int rBlkIdx, vector<int> off1, vector<int> off2);
    void generate_butterfly_parity_decoding_matrix_3(int *parity_dec, int rBlkIdx, vector<int> off);
    void generate_butterfly_parity_decoding_matrix_4(int *parity_dec);
    void dump_matrix(int* matrix, int row, int col);
  public:
    BUTTERFLY(int n, int k, int cps, vector<string> param);
//    BUTTERFLY(int n, int k, int cps, int opt, vector<string> param);
    ECDAG* Encode();
    ECDAG* Decode(vector<int> from, vector<int> to);
 //   void Place(vector<vector<int>>& group);
};


#endif
