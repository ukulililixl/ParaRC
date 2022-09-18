#include "HHXORPlus.hh"

HHXORPlus::HHXORPlus(int n, int k, int w, vector<string> param) {
    
    _n = n;
    _k = k;
    _w = w;

    // by default (as base code), num_instances = 1, instance_id = 0
    int num_instances = 1;
    int instance_id = 0;

    if (param.size() > 1) {
        num_instances = atoi(param[0].c_str()); // total number of instances
        instance_id = atoi(param[1].c_str()); // current instance id
    }

    if (_w != 2) {
        printf("error: w != 2\n");
        return;
    }

    if (_n - _k < 3) {
        printf("error: n - k should >= 3\n");
        return;
    }

    // generate encoding matrix
    _rs_encode_matrix = (int *)malloc(_n * _k * sizeof(int));
    generate_vandermonde_matrix(_rs_encode_matrix, _n, _k, 8);
    // generate_cauchy_matrix(_rs_encode_matrix, _n, _k, 8);

    // divide data into groups
    int num_groups = n - k - 1;

    if (_k < num_groups * 2) {
        printf("error: k should >= (n - k - 1) * 2\n");
        return;
    }

    int avg_group_size = _k / num_groups;
    // Note: remove the last element out of the last group!
    int last_group_size = _k - avg_group_size * (num_groups - 1) - 1;

    for (int i = 0, data_idx = 0; i < num_groups; i++) {
        int parity_idx = i + 1;
        int group_size = (i < num_groups - 1) ? avg_group_size : last_group_size;
        for (int did = 0; did < group_size; did++) {
            _pid_group_map[parity_idx].push_back(data_idx);
            data_idx++;
        }

        // printf("group[%d]: ", parity_idx);
        // for (int did = 0; did < group_size; did++) {
        //     printf("%d ", _pid_group_map[parity_idx].at(did));
        // }
        // printf("\n");
    }

    // total number of virtual symbols
    _num_symbols = _n * _w + 2 * (_pid_group_map.size() + 1);
    // printf("num_symbols: %d\n", _num_symbols);
    
    // initialize layout
    init_layout(num_instances, instance_id);

    SetLayout(_layout);
    SetSymbols(_symbols);

    // printf("layout:\n");
    // for (int sp = 0; sp < _w; sp++) {
    //     for (int i = 0; i < _n; i++) {
    //         printf("%d ", _layout[sp][i]);
    //     }
    //     printf("\n");
    // }
    // printf("\n");

    // printf("data layout:\n");
    // for (int sp = 0; sp < _w; sp++) {
    //     for (int i = 0; i < _k; i++) {
    //         printf("%d ", _data_layout[sp][i]);
    //     }
    //     printf("\n");
    // }
    // printf("\n");

    // printf("code layout:\n");
    // for (int sp = 0; sp < _w; sp++) {
    //     for (int i = 0; i < _n - _k; i++) {
    //         printf("%d ", _code_layout[sp][i]);
    //     }
    //     printf("\n");
    // }
    // printf("\n");

    // printf("uncoupled code layout:\n");
    // for (int sp = 0; sp < _w; sp++) {
    //     for (int i = 0; i < _n - _k; i++) {
    //         printf("%d ", _uncoupled_code_layout[sp][i]);
    //     }
    //     printf("\n");
    // }
    // printf("\n");

    // printf("group code layout:\n");
    // for (auto const &pid_group_code : _pid_group_code_map) {
    //     printf("pid: %d, code: %d\n", pid_group_code.first, pid_group_code.second);
    // }
    // printf("\n");

    /**
     * @brief swap the first and second row of encode matrix
     * Originally, the first row are all 1, move to the second row
     */
    int *tmp_row = (int *)malloc(_k * sizeof(int));
    memcpy(tmp_row, &_rs_encode_matrix[_k * _k], _k * sizeof(int));
    memcpy(&_rs_encode_matrix[_k * _k], &_rs_encode_matrix[(_k + 1) * _k], _k * sizeof(int));
    memcpy(&_rs_encode_matrix[(_k + 1) * _k], tmp_row, _k * sizeof(int));
    free(tmp_row);

    // printf("info: Hitchhiker-XORPlus(%d, %d) initialized\n", _n, _k);
}

HHXORPlus::~HHXORPlus() {
    free(_rs_encode_matrix);
}

void HHXORPlus::generate_vandermonde_matrix(int* matrix, int rows, int cols, int w) {
    int k = cols;
    int n = rows;
    int m = n - k;

    memset(matrix, 0, rows * cols * sizeof(int));
    for(int i=0; i<k; i++) {
        matrix[i*k+i] = 1;
    }

    // Vandermonde matrix
    for (int i=0; i<m; i++) {
        int tmp = 1;
        for (int j=0; j<k; j++) {
        matrix[(i+k)*cols+j] = tmp;
        tmp = Computation::singleMulti(tmp, i+1, w);
        }
    }
}

void HHXORPlus::generate_cauchy_matrix(int* matrix, int rows, int cols, int w) {
    int k = cols;
    int n = rows;
    int m = n - k;

    memset(matrix, 0, rows * cols * sizeof(int));
    for(int i=0; i<k; i++) {
        matrix[i*k+i] = 1;
    }

    // Cauchy matrix
    int *p = &matrix[_k * _k];
    for (int i = k; i < n; i++) {
		for (int j = 0; j < k; j++) {
			*p++ = galois_single_divide(1, i ^ j, 8);
        }
    }
}

ECDAG* HHXORPlus::Encode() {
    ECDAG* ecdag = new ECDAG();

    Encode(ecdag);

    return ecdag;
}

void HHXORPlus::Encode(ECDAG *ecdag) {
    if (ecdag == NULL) {
        printf("error: invalid input ecdag\n");
        return;
    }

    // calculate uncoupled parity
    for (int sp = 0; sp < _w; sp++) {
        vector<int> &data_layout = _data_layout[sp];
        vector<int> &uncoupled_code_layout = _uncoupled_code_layout[sp];
        
        for (int i = 0; i < _n - _k; i++) {
            vector<int> coefs(&_rs_encode_matrix[(i + _k) * _k], _rs_encode_matrix + (i + _k + 1) * _k);
            ecdag->Join(uncoupled_code_layout[i], data_layout, coefs);
        }

    }

    // calculate group code
    int num_groups = _pid_group_map.size();
    for (auto const &pid_group : _pid_group_map) {
        int parity_idx = pid_group.first;
        const vector<int> &group = pid_group.second;
        int pidx = _pid_group_code_map[parity_idx];
        int group_size = group.size();

        vector<int> cidx_group;
        for (auto node_id : group) {
            cidx_group.push_back(_layout[0][node_id]);
        }
        
        vector<int> coefs_group(group_size);
        for (int i = 0; i < group_size; i++) {
            coefs_group[i] = _rs_encode_matrix[(_k + couple_parity_id) * _k + group[i]];
        }

        ecdag->Join(pidx, cidx_group, coefs_group);

    }

    // couple the only parity in sp[0]
    ecdag->Join(_code_layout[0][couple_parity_id],
        {_uncoupled_code_layout[0][couple_parity_id],
            _uncoupled_code_layout[1][couple_parity_id],
            _pid_group_code_map[couple_parity_id]},
        {1, 1, 1});

    // couple the parities in sp[1]
    for (auto const &pid_group : _pid_group_map) {
        int parity_idx = pid_group.first;
        ecdag->Join(_code_layout[1][parity_idx],
        {_uncoupled_code_layout[1][parity_idx],
            _pid_group_code_map[parity_idx]},
        {1, 1});
    }
}

ECDAG* HHXORPlus::Decode(vector<int> from, vector<int> to) {
    ECDAG* ecdag = new ECDAG();

    Decode(from, to, ecdag);

    return ecdag;
}

void HHXORPlus::Decode(vector<int> from, vector<int> to, ECDAG *ecdag) {
    // num_lost_symbols * w lost symbols
    if (from.size() % _w != 0 || to.size() % _w != 0) {
        printf("error: invalid number of symbols\n");
        return;
    }

    int num_failed_nodes = to.size() / _w;

    if (num_failed_nodes == 1) {
        DecodeSingle(from, to, ecdag);
    } else {
        DecodeMultiple(from, to, ecdag);
    }
}

// void HHXORPlus::Place(vector<vector<int>>& group) {
//     return;
// }

void HHXORPlus::DecodeSingle(vector<int> from, vector<int> to, ECDAG *ecdag) {
    
    if (ecdag == NULL) {
        printf("error: invalid input ecdag\n");
        return;
    }

    int failed_node = to[0] / _w; // failed node id

    for (int w = 0; w < _w; w++) {
        for (int node_id = 0; node_id < _n; node_id++) {
            if (to[0] == _layout[w][node_id]) {
                failed_node = node_id;
                break;
            }
        }
    }
    if (failed_node >= _k) { // parity node failure
        DecodeMultiple(from, to, ecdag); // resort to conventional repair
        return;
    }

    // create node_syms_map: <node_id, <symbol_0, symbol_1, ...>>
    map<int, vector<int>> node_syms_map; // ordered by node id
    for (auto symbol : from) {
        int alive_node = -1;
        for (int w = 0; w < _w; w++) {
            for (int node_id = 0; node_id < _n; node_id++) {
                if (symbol == _layout[w][node_id]) {
                    alive_node = node_id;
                    break;
                }
            }
        }

        if (alive_node != -1) {
            node_syms_map[alive_node].push_back(symbol);
        }
    }

    vector<int> avail_data_node; // available data node
    vector<int> avail_parity_node; // available parity node
    for (auto node_syms : node_syms_map) {
        int node_id = node_syms.first;
        if (node_id < _k) {
            avail_data_node.push_back(node_id);
        } else {
            avail_parity_node.push_back(node_id);
        }
    }

    // data node failure
    int *recover_matrix = (int *) malloc(_k * _k * sizeof(int));
    int *recover_matrix_inv = (int *) malloc(_k * _k * sizeof(int));
    int *select_vector = (int *) malloc(_k * sizeof(int));

    for (int i = 0; i < avail_data_node.size(); i++) {
        memcpy(&recover_matrix[i * _k], &_rs_encode_matrix[avail_data_node[i] * _k], _k * sizeof(int));
    }

    // copy the first parity function
    memcpy(&recover_matrix[(_k - 1) * _k], &_rs_encode_matrix[_k * _k], _k * sizeof(int));

    int mtx_invertible = jerasure_invert_matrix(recover_matrix, recover_matrix_inv, _k, 8);

    if (mtx_invertible == -1) {
        printf("error: recover_matrix not invertible!\n");
    }

    // get decode_vector for data symbol in sp[1]
    memcpy(select_vector, &recover_matrix_inv[failed_node * _k], _k * sizeof(int));

    vector<int> cidx_sp1;
    for (auto node_id : avail_data_node) {
        cidx_sp1.push_back(_layout[1][node_id]);
    }
    cidx_sp1.push_back(_layout[1][_k]); // append the first parity

    // recover data symbol in sp[1]
    vector<int> coefs_data_sp1(select_vector, select_vector + _k);
    ecdag->Join(_data_layout[1][failed_node], cidx_sp1, coefs_data_sp1);

    if (failed_node < _k - 1) { // first k - 1 node    
        // locate the group
        for (auto const &pid_group : _pid_group_map) {
            const vector<int> &group = pid_group.second;
            if (std::find(group.begin(), group.end(), failed_node) == group.end()) {
                continue;
            }
            int group_size = group.size();
            int parity_idx = pid_group.first;

            // get decode_vector for uncoupled parity symbol in sp[1]
            memcpy(select_vector, &_rs_encode_matrix[(_k + parity_idx) * _k], _k * sizeof(int));

            int *decode_parity_vector = jerasure_matrix_multiply(
                select_vector, recover_matrix_inv, 1, _k, _k, _k, 8);

            // recover uncoupled parity symbol in sp[1]
            vector<int> coefs_parity_sp1(decode_parity_vector, decode_parity_vector + _k);
            ecdag->Join(_uncoupled_code_layout[1][parity_idx], cidx_sp1, coefs_parity_sp1);

            // XOR the coupled parity to get group code
            ecdag->Join(_pid_group_code_map[parity_idx],
                {_uncoupled_code_layout[1][parity_idx], _code_layout[1][parity_idx]},
                {1, 1});

            // recover data symbol in sp[0] by solving f(couple_parity_id)
            int *recover_matrix_group = (int *) malloc(group_size * group_size * sizeof(int));
            int *recover_matrix_group_inv = (int *) malloc(group_size * group_size * sizeof(int));
            int *select_vector_group = (int *) malloc(group_size * sizeof(int));
            
            int in_group_idx = -1;
            memset(recover_matrix_group, 0, group_size * group_size * sizeof(int));
            for (int i = 0, row_idx = 0; i < group_size; i++) {
                if (group[i] != failed_node) {
                    recover_matrix_group[row_idx * group_size + i] = 1;
                    row_idx++;
                } else {
                    in_group_idx = i;
                }
            }
            for (int i = 0; i < group_size; i++) {
                recover_matrix_group[(group_size - 1) * group_size + i] = _rs_encode_matrix[(_k + couple_parity_id) * _k + group[i]];
            }

            mtx_invertible = jerasure_invert_matrix(recover_matrix_group, recover_matrix_group_inv, group_size, 8);

            if (mtx_invertible == -1) {
                printf("error: recover_matrix not invertible!\n");
            }

            memcpy(select_vector_group, &recover_matrix_group_inv[in_group_idx * group_size], group_size * sizeof(int));

            vector<int> cidx_group;
            for (auto node_id : group) {
                if (node_id != failed_node) {
                    cidx_group.push_back(_layout[0][node_id]);
                }
            }
            cidx_group.push_back(_pid_group_code_map[parity_idx]);
            vector<int> coefs_group(select_vector_group, select_vector_group + group_size);
            
            ecdag->Join(_data_layout[0][failed_node], cidx_group, coefs_group);
            
            free(decode_parity_vector);
            free(recover_matrix_group);
            free(recover_matrix_group_inv);
            free(select_vector_group);
        }

    } else { // special handling for the last data node

        for (auto const &pid_group : _pid_group_map) {
            const vector<int> &group = pid_group.second;
            int group_size = group.size();
            int parity_idx = pid_group.first;

            // get decode_vector for uncoupled parity symbol in sp[1]
            memcpy(select_vector, &_rs_encode_matrix[(_k + parity_idx) * _k], _k * sizeof(int));
            int *decode_parity_vector = jerasure_matrix_multiply(
                select_vector, recover_matrix_inv, 1, _k, _k, _k, 8);

            // recover uncoupled parity symbol in sp[1]
            vector<int> coefs_parity_sp1(decode_parity_vector, decode_parity_vector + _k);
            ecdag->Join(_uncoupled_code_layout[1][parity_idx], cidx_sp1, coefs_parity_sp1);            
        }
        
        // XOR couples
        vector<int> cidx_couples;
        vector<int> coefs_couples;
        for (auto const &pid_group_code : _pid_group_code_map) {
            int parity_idx = pid_group_code.first;
            int uncoupled_code = _uncoupled_code_layout[1][parity_idx];
            cidx_couples.push_back(uncoupled_code);
            if (parity_idx != couple_parity_id) {
                cidx_couples.push_back(_code_layout[1][parity_idx]);
            } else {
                cidx_couples.push_back(_code_layout[0][parity_idx]);
            }
            coefs_couples.push_back(1);
            coefs_couples.push_back(1);                
        }

        // int pidx = _uncoupled_code_layout[1][_n - _k - 1] + 1; // add a new virtual symbol
        int pidx = _symbols[_num_symbols - 1]; // use the last virtual symbol
        ecdag->Join(pidx, cidx_couples, coefs_couples);

        int coef = galois_single_divide(
            1, _rs_encode_matrix[(_k + couple_parity_id) * _k + failed_node], 8);

        ecdag->Join(_data_layout[0][failed_node], {pidx}, {coef});

    }

    free(recover_matrix);
    free(recover_matrix_inv);
    free(select_vector);
}

void HHXORPlus::DecodeMultiple(vector<int> from, vector<int> to, ECDAG *ecdag) {

    if (ecdag == NULL) {
        printf("error: invalid input ecdag\n");
        return;
    }

    // create avail_node_syms_map: <node_id, <symbol_0, symbol_1, ...>>
    map<int, vector<int>> avail_node_syms_map; // ordered by node id
    for (auto symbol : from) {
        int avail_node = -1;
        for (int w = 0; w < _w; w++) {
            for (int node_id = 0; node_id < _n; node_id++) {
                if (symbol == _layout[w][node_id]) {
                    avail_node = node_id;
                    break;
                }
            }
        }
        
        if (avail_node != -1) {
            avail_node_syms_map[avail_node].push_back(symbol);
        }
    }

    // create lost_node_syms_map: <node_id, <symbol_0, symbol_1, ...>>
    map<int, vector<int>> lost_node_syms_map; // ordered by node id
    for (auto symbol : to) {
        int failed_node = -1;
        for (int w = 0; w < _w; w++) {
            for (int node_id = 0; node_id < _n; node_id++) {
                if (symbol == _layout[w][node_id]) {
                    failed_node = node_id;
                    break;
                }
            }
        }
        lost_node_syms_map[failed_node].push_back(symbol);
    }
    int num_lost_nodes = lost_node_syms_map.size();

    // available node
    vector<int> avail_data_node; // available data node
    vector<int> avail_parity_node; // available parity node
    int num_avail_data = 0;
    int num_avail_parity = 0;

    // append all data
    for (auto avail_node_syms : avail_node_syms_map) {
        int node_id = avail_node_syms.first;
        if (node_id < _k) {
            avail_data_node.push_back(node_id);
            num_avail_data++;
        }
    }

    // append parities until (data + parity = k)
    for (auto avail_node_syms : avail_node_syms_map) {
        int node_id = avail_node_syms.first;
        if (node_id >= _k && num_avail_data + num_avail_parity < _k) {
            avail_parity_node.push_back(node_id);
            num_avail_parity++;
        }
    }

    // make sure we use k nodes only
    assert(num_avail_data + num_avail_parity == _k);

    // lost node
    vector<int> lost_data_node; // lost data node
    vector<int> lost_parity_node; // lost parity node
    for (auto lost_node_syms : lost_node_syms_map) {
        int node_id = lost_node_syms.first;
        if (node_id < _k) {
            lost_data_node.push_back(node_id);
        } else {
            lost_parity_node.push_back(node_id);
        }
    }

    for (auto node_id : avail_data_node) {
        printf("avail_data: %d\n", node_id);
    }

    for (auto node_id : avail_parity_node) {
        printf("avail_parity: %d\n", node_id);
    }

    for (auto node_id : lost_data_node) {
        printf("lost_data: %d\n", node_id);
    }

    for (auto node_id : lost_parity_node) {
        printf("lost_parity: %d\n", node_id);
    }

    // special handling for coupled item
    if (std::find(avail_parity_node.begin(), avail_parity_node.end(), couple_parity_id + _k) != avail_parity_node.end()) {
        ecdag->Join(_uncoupled_code_layout[0][couple_parity_id],
            {_code_layout[0][couple_parity_id], _code_layout[1][couple_parity_id]},
            {1, 1});

    }

    // recover all symbols in sp[0]
    int *recover_matrix = (int *) malloc(_k * _k * sizeof(int));
    int *recover_matrix_inv = (int *) malloc(_k * _k * sizeof(int));
    int *select_vector = (int *) malloc(_k * sizeof(int));

    // copy the first parity function
    for (int i = 0; i < avail_data_node.size(); i++) {
        memcpy(&recover_matrix[i * _k], &_rs_encode_matrix[avail_data_node[i] * _k], _k * sizeof(int));
    }

    for (int i = 0; i < avail_parity_node.size(); i++) {
        memcpy(&recover_matrix[(num_avail_data + i) * _k], &_rs_encode_matrix[avail_parity_node[i] * _k], _k * sizeof(int));
    }

    int mtx_invertible = jerasure_invert_matrix(recover_matrix, recover_matrix_inv, _k, 8);

    if (mtx_invertible == -1) {
        printf("error: recover_matrix not invertible!\n");
    }

    // cidx sp[0]
    vector<int> cidx_sp0;
    for (auto node_id : avail_data_node) {
        cidx_sp0.push_back(_layout[0][node_id]);
    }
    
    for (auto node_id : avail_parity_node) {
        cidx_sp0.push_back(_uncoupled_code_layout[0][node_id - _k]);
    }

    for (auto lost_node_syms : lost_node_syms_map) {
        // get decode_vector in sp[0]
        int failed_node = lost_node_syms.first;
        memcpy(select_vector, &_rs_encode_matrix[failed_node * _k], _k * sizeof(int));

        int *decode_parity_vector = jerasure_matrix_multiply(
            select_vector, recover_matrix_inv, 1, _k, _k, _k, 8);
        
        vector<int> coefs_parity_sp0(decode_parity_vector, decode_parity_vector + _k);

        if (failed_node < _k) {
            ecdag->Join(_data_layout[0][failed_node], cidx_sp0, coefs_parity_sp0);
        } else { // for parity, calculate uncoupled code and group code
            int parity_idx = failed_node - _k;

            // calculate uncoupled code
            ecdag->Join(_uncoupled_code_layout[0][parity_idx], cidx_sp0, coefs_parity_sp0);

            if (_pid_group_map.find(parity_idx) != _pid_group_map.end()) {
                // calculate group code
                const vector<int> &group = _pid_group_map[parity_idx];
                int pidx = _pid_group_code_map[parity_idx];
                int group_size = group.size();

                vector<int> cidx_group;
                for (auto node_id : group) {
                    cidx_group.push_back(_layout[0][node_id]);
                }
                
                vector<int> coefs_group(group_size);
                for (int i = 0; i < group_size; i++) {
                    coefs_group[i] = _rs_encode_matrix[(_k + couple_parity_id) * _k + group[i]];
                }

                ecdag->Join(pidx, cidx_group, coefs_group);

            }
        }

        free(decode_parity_vector);
    }

    // recover uncoupled items for sp[1]
    for (int i = 0; i < num_avail_parity; i++) {
        int parity_idx = avail_parity_node[i] - _k;

        if (_pid_group_map.find(parity_idx) != _pid_group_map.end()) {
            // calculate group code
            const vector<int> &group = _pid_group_map[parity_idx];
            int group_size = group.size();
            int pidx = _pid_group_code_map[parity_idx];
            
            vector<int> cidx_group;
            for (auto node_id : group) {
                cidx_group.push_back(_layout[0][node_id]);
            }

            vector<int> coefs_group(group_size);
            for (int i = 0; i < group_size; i++) {
                coefs_group[i] = _rs_encode_matrix[(_k + couple_parity_id) * _k + group[i]];
            }
            ecdag->Join(pidx, cidx_group, coefs_group);

            ecdag->Join(_uncoupled_code_layout[1][parity_idx],
                {_code_layout[1][parity_idx], pidx},
                {1, 1});

        }
    }

    // cidx sp[1]
    vector<int> cidx_sp1;
    for (auto node_id : avail_data_node) {
        cidx_sp1.push_back(_data_layout[1][node_id]);
    }
    for (auto node_id : avail_parity_node) {
        cidx_sp1.push_back(_uncoupled_code_layout[1][node_id - _k]);
    }

    for (auto lost_node_syms : lost_node_syms_map) {
        // get decode_vector in sp[1]
        int failed_node = lost_node_syms.first;
        memcpy(select_vector, &_rs_encode_matrix[failed_node * _k], _k * sizeof(int));
        int *decode_parity_vector = jerasure_matrix_multiply(
            select_vector, recover_matrix_inv, 1, _k, _k, _k, 8);
        
        vector<int> coefs_parity_sp1(decode_parity_vector, decode_parity_vector + _k);

        if (failed_node < _k) {
            ecdag->Join(_data_layout[1][failed_node], cidx_sp1, coefs_parity_sp1);
        } else { // for parity, calculate uncoupled code
            ecdag->Join(_uncoupled_code_layout[1][failed_node - _k], cidx_sp1, coefs_parity_sp1);
        }

        free(decode_parity_vector);
    }

    // re-couple the only parity in sp[0]
    if (std::find(lost_parity_node.begin(), lost_parity_node.end(), couple_parity_id + _k) != lost_parity_node.end()) {
        ecdag->Join(_code_layout[0][couple_parity_id],
            {_uncoupled_code_layout[0][couple_parity_id],
                _uncoupled_code_layout[1][couple_parity_id],
                _pid_group_code_map[couple_parity_id]},
            {1, 1, 1});

    }

    // couple the parities in sp[1]
    for (int i = 0; i < lost_parity_node.size(); i++) {
        int parity_idx = lost_parity_node[i] - _k;

        if (_pid_group_map.find(parity_idx) != _pid_group_map.end()) {
            ecdag->Join(_code_layout[1][parity_idx],
            {_uncoupled_code_layout[1][parity_idx],
                _pid_group_code_map[parity_idx]},
            {1, 1});
            
        }

    }
}

vector<int> HHXORPlus::GetNodeSymbols(int nodeid) {
    vector<int> symbols;
    for (int i = 0; i < _w; i++) {
        symbols.push_back(_layout[i][nodeid]);
    }

    return symbols;
}

vector<vector<int>> HHXORPlus::GetLayout() {
    return _layout;
}

int HHXORPlus::GetNumSymbols() {
    return _num_symbols;
}

void HHXORPlus::init_layout(int num_instances, int instance_id) {

    // _num_symbols has already been initialized in HHXORPlus()
    int num_layout_symbols = _n * _w; // number of symbols in layout of one instance
    int num_additional_symbols = _num_symbols - num_layout_symbols; // number of additional symbols in one instance

    int num_global_symbols = _num_symbols * num_instances;
    int num_global_layout_symbols = num_layout_symbols * num_instances;
    int num_global_additional_symbols = num_additional_symbols * num_instances;
    
    vector<vector<int>> global_symbols(num_instances);
    vector<vector<int>> global_layout(num_instances * _w);

    // assign layout symbols to each instance
    for (int ins_id = 0; ins_id < num_instances; ins_id++) {
        for (int sp = 0; sp < _w; sp++) {
            for (int node_id = 0; node_id < _n; node_id++) {
                int symbol = node_id * _w * num_instances + ins_id * _w + sp;
                global_layout[ins_id * _w + sp].push_back(symbol);
                global_symbols[ins_id].push_back(symbol);
            }
        }
    }

    // assign additional symbols to each instance
    int vs_id = num_global_layout_symbols;
    for (int ins_id = 0; ins_id < num_instances; ins_id++) {
        for (int i = 0; i < num_additional_symbols; i++) {
            global_symbols[ins_id].push_back(vs_id++);
        }
    }

    _layout.clear();
    for (int i = 0; i < _w; i++) {
        _layout.push_back(global_layout[instance_id * _w + i]);
    }

    _symbols = global_symbols[instance_id];
}

void HHXORPlus::SetLayout(vector<vector<int>> layout) {
    if (layout.size() != _layout.size()) {
        printf("invalid layout\n");
        return;
    }

    _layout = layout;

    // handle internal layouts
    _data_layout.clear();
    _code_layout.clear();

    for (int sp = 0; sp < _w; sp++) {
        vector<int> &layout = _layout[sp];
        vector<int> data_layout;
        vector<int> code_layout;

        for (int i = 0; i < _n; i++) {
            if (i < _k) {
                data_layout.push_back(layout[i]);
            } else {
                code_layout.push_back(layout[i]);
            }
        }
        
        _data_layout.push_back(data_layout);
        _code_layout.push_back(code_layout);
    }

    _uncoupled_code_layout = _code_layout;

    // printf("HHXORPlus::SetLayout: \n");
    // for (int sp = 0; sp < _w; sp++) {
    //     for (int i = 0; i < _n; i++) {
    //         printf("%d ", _layout[sp][i]);
    //     }
    //     printf("\n");
    // }

}

void HHXORPlus::SetSymbols(vector<int> symbols) {
    if (symbols.size() != _num_symbols) {
        printf("invalid symbols\n");
        return;
    }

    _symbols = symbols;

    // handle internal symbols

    // handle sp[0]
    int vs_id = _n * _w; // virtual symbol id
    _uncoupled_code_layout[0][couple_parity_id] = _symbols[vs_id++];
    for (auto const &pid_group : _pid_group_map) {
        int parity_idx = pid_group.first;
        _pid_group_code_map[parity_idx] = _symbols[vs_id++];
    }

    // handle sp[1]
    for (auto const &pid_group : _pid_group_map) {
        int parity_idx = pid_group.first;
        _uncoupled_code_layout[1][parity_idx] = _symbols[vs_id++];
    }

    // printf("HHHHXORPlus::SetSymbols: \n");
    // for (int i = 0; i < _num_symbols; i++) {
    //     printf("%d ", _symbols[i]);
    // }
    // printf("\n");
}

vector<int> HHXORPlus::GetRequiredSymbolsSingle(int failed_node) {
    vector<int> required_symbols;

    if (failed_node < _k) {

        for (int node_id = 0; node_id < _k; node_id++) {
            if (node_id == failed_node) {
                continue;
            }
            required_symbols.push_back(_layout[1][node_id]);
        }

        required_symbols.push_back(_layout[1][_k]);

        if (failed_node < _k - 1) {
            int failed_pid = -1;
            
            for (auto group_map : _pid_group_map) {
                for (auto node_id : group_map.second) {
                    if (node_id == failed_node) {
                        failed_pid = group_map.first;
                        break;
                    }
                }
            }

            for (auto node_id : _pid_group_map[failed_pid]) {
                if (node_id == failed_node) {
                    continue;
                }
                required_symbols.push_back(_layout[0][node_id]);
            }

            required_symbols.push_back(_layout[1][_k + failed_pid]);
        } else {
            for (auto group_map : _pid_group_map) {
                // skip the first group
                if (group_map.first == 1) {
                    continue;
                }
                required_symbols.push_back(_layout[1][_k + group_map.first]);
            }
            required_symbols.push_back(_layout[0][_k + couple_parity_id]);
        }
    } else {
        for (int w = 0; w < _w; w++) {
            for (int node_id = 0; node_id < _k; node_id++) {
                required_symbols.push_back(_layout[w][node_id]);
            }
        }
    }

    return required_symbols;
}

vector<int> HHXORPlus::GetRequiredParitySymbolsSingle(int failed_node) {
    vector<int> required_all_symbols = GetRequiredSymbolsSingle(failed_node);

    printf("required_all_symbols:\n");            
    for (auto symbol : required_all_symbols) {
        printf("%d ", symbol);
    }
    printf("\n");

    vector<int> required_symbols;

    for (auto symbol : required_all_symbols) {
        for (int w = 0; w < _w; w++) {
            for (int node_id = _k; node_id < _n; node_id++) {
                if (_layout[w][node_id] == symbol) {
                    required_symbols.push_back(symbol);
                    break;
                }
            }
        }
    }

    return required_symbols;
}
