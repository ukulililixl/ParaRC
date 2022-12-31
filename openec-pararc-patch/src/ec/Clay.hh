#ifndef _CLAY_HH_
#define _CLAY_HH_

//#include "../inc/include.hh"
#include "Computation.hh"

#include "ECBase.hh"

using namespace std;

#define CLAY_DEBUG_ENABLE false
#define CLAYPFT_N_MAX (32)
#define CLAYMDS_N_MAX (32)

class Erasure_t {
    public: 
        int _x;
        int _y;

        Erasure_t(int x, int y);
        void dump();
};

class Clay: public ECBase {
    private:
        int _m; 
        int _d;
        int _q;
        int _t;
        int _nu = 0;

        // for mds
        int _mds_k;
        int _mds_m;
        int _mds_encode_matrix[CLAYMDS_N_MAX * CLAYMDS_N_MAX];

        // for pft
        int _pft_k;
        int _pft_m;
        int _pft_encode_matrix[CLAYPFT_N_MAX * CLAYPFT_N_MAX];

        int _sub_chunk_no;

        // for shortening?
        unordered_map<int, int> _short2real;
        unordered_map<int, int> _real2short;

        void generate_matrix(int* matrix, int rows, int cols, int w);
        int pow_int(int a, int x);
        void get_erasure_coordinates(vector<int> erased_chunk, Erasure_t** erasures);
        void get_weight_vector(Erasure_t** erasures, int* weight_vec);
        int get_hamming_weight(int* weight_vec);
        void set_planes_sequential_decoding_order(int* order, Erasure_t** erasures);
        void get_plane_vector(int z, int* z_vec);

        void decode_erasures(vector<int> erased_chunks, int z, ECDAG* ecdag);
        vector<int> get_uncoupled_from_coupled(int x, int y, int z, int* z_vec, ECDAG* ecdag);
        vector<int> decode_uncoupled(vector<int> erased_chunks, int z, ECDAG* ecdag);
        void get_coupled_from_uncoupled(int x, int y, int z, int* z_vec, ECDAG* ecdag);

        void print_matrix(int* matrix, int row, int col);

        int is_repair(vector<int> want_to_read, vector<int> avail);
        void minimum_to_repair(vector<int> want_to_read, vector<int> available_chunks, unordered_map<int, vector<pair<int, int>>>& minimum);
        void get_repair_subchunks(int lost_node, vector<pair<int, int>> &repair_sub_chunks_ind);
        int get_repair_sub_chunk_count(vector<int> want_to_read);
        void repair_one_lost_chunk(unordered_map<int, bool>& recovered_data,
                vector<int>& aloof_nodes,
                unordered_map<int, bool>& helper_data,
                vector<pair<int,int>> &repair_sub_chunks_ind,
                ECDAG* ecdag);

        vector<int> get_coupled1_from_pair02(int x, int y, int z, int* z_vec, ECDAG* ecdag);
        vector<int> get_coupled0_from_pair13(int x, int y, int z, int* z_vec, ECDAG* ecdag);

        int get_real_from_short(int idx);
        int get_short_from_real(int idx);

    public:
        // Clay(int n, int k, int w, vector<string> param);
       Clay(int n, int k, int w, int opt, vector<string> param);
//
        ECDAG* Encode();
        ECDAG* Decode(vector<int> from, vector<int> to);
       void Place(vector<vector<int>>& group);
//        void Shorten(unordered_map<int, int>& shortening);

};

#endif
