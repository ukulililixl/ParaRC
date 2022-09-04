#ifndef __HH_XOR_PLUS_HH__
#define __HH_XOR_PLUS_HH__

#include <map>

#include "Computation.hh"
#include "ECBase.hh"

class HHXORPlus : public ECBase
{
private:

    int *_rs_encode_matrix; // underlying RS encode matrix
    
    vector<vector<int>> _layout; // layout (w * n)
    vector<vector<int>> _data_layout; // data layout (w * k)
    vector<vector<int>> _code_layout; // code layout (w * (n - k))
    vector<vector<int>> _uncoupled_code_layout; // uncoupled code layout (w * (n - k))
    
    map<int, int> _pid_group_code_map; // parity_id to group code map
    map<int, vector<int>> _pid_group_map; // parity_id to data group map

    int couple_parity_id = 1; // following the paper, f2 is used for coupling in sp[0]

    int _num_symbols; // total number of symbols (k * w + num_groups * w + 2)
    vector<int> _symbols; // virtual symbols (n)

    /**
     * @brief generate rows * cols Vandermonde matrix in GF(2^w)
     * 
     * @param matrix 
     * @param rows 
     * @param cols 
     * @param w 
     */
    void generate_vandermonde_matrix(int* matrix, int rows, int cols, int w);

    /**
     * @brief generate rows * cols Cauchy matrix in GF(2^w)
     * 
     * @param matrix 
     * @param rows 
     * @param cols 
     * @param w 
     */
    void generate_cauchy_matrix(int* matrix, int rows, int cols, int w);

    /**
     * @brief initialize layout and symbols by instance id (out of num_instances)
     * 
     * @param num_instances total number of instances
     * @param instance_id current instance id
     */
    void init_layout(int num_instances, int instance_id);

     /**
     * @brief Decode ECDAG for single failure
     * 
     * @param from available symbols
     * @param to lost symbols
     * @param ecdag 
     */
    void DecodeSingle(vector<int> from, vector<int> to, ECDAG *ecdag);

    /**
     * @brief Decode ECDAG for multiple failures
     * 
     * @param from available symbols
     * @param to lost symbols
     * @param ecdag 
     */
    void DecodeMultiple(vector<int> from, vector<int> to, ECDAG *ecdag);

public:

    /**
     * @brief Constructor
     * 
     * @param n ec n
     * @param k ec k
     * @param sub-packetization (fixed to 2)
     * @param param additional parameters
     */
    HHXORPlus(int n, int k, int w, vector<string> param);

    /**
     * @brief Destructor
     * 
     */
    ~HHXORPlus();

    /**
     * @brief construct Encode ECDAG
     * 
     * @return ECDAG* 
     */
    ECDAG* Encode();

    /**
     * @brief Decode ECDAG
     * 
     * @param from available symbols
     * @param to lost symbols
     * @return ECDAG* 
     */
    ECDAG* Decode(vector<int> from, vector<int> to);

    // /**
    //  * @brief Not implemented
    //  * 
    //  * @param group 
    //  */
    // void Place(vector<vector<int>>& group);

    /**
     * @brief append encode routine on ecdag
     * 
     * @param ecdag 
     */
    void Encode(ECDAG *ecdag);

    /**
     * @brief append decode routine on ecdag
     * 
     * @param from 
     * @param to 
     * @param ecdag 
     */
    void Decode(vector<int> from, vector<int> to, ECDAG *ecdag);

    /**
     * @brief Get symbols in nodeid
     * 
     * @param nodeid 
     * @return vector<int> 
     */
    vector<int> GetNodeSymbols(int nodeid);

    /**
     * @brief Get code layout
     * 
     * 0 2 4 6 8 ...
     * 1 3 5 7 9 ... 
     * 
     * @return vector<vector<int>> 
     */
    vector<vector<int>> GetLayout();

    /**
     * @brief Get total number of symbols
     * 
     * @return int 
     */
    int GetNumSymbols();

    /**
     * @brief Set code layout 
     * 
     * @param vector<vector<int>> 
     * 0 2 4 6 8 ...
     * 1 3 5 7 9 ... 
     * 
     */
    void SetLayout(vector<vector<int>> layout);
    
    /**
     * @brief Set code layout 
     * 
     * @param vector<int> symbols (size equal to num_symbols)
     * the first few symbols are symbols in the layout, the following are required internal symbols 
     * 0 2 4 6 8 ...
     * 1 3 5 7 9 ... 
     * 
     */
    void SetSymbols(vector<int> symbols);

    /**
     * @brief Get required symbols for single failure repair
     * 
     * @param failed_node 
     * @return vector<int> symbols in layout
     */
    vector<int> GetRequiredSymbolsSingle(int failed_node);

    /**
     * @brief Get required parity symbols for single failure repair
     * 
     * @param failed_node 
     * @return vector<int> symbols in layout
     */
    vector<int> GetRequiredParitySymbolsSingle(int failed_node);
};

#endif // __HH_XOR_PLUS_HH__