#include "Clay.hh"

Erasure_t::Erasure_t(int x, int y) {
    _x = x;
    _y = y;
}

void Erasure_t::dump() {
    cout << "(" << _x << ", " << _y << ")" << endl;
}

//Clay::Clay(int n, int k, int w, int opt, vector<string> param) {
Clay::Clay(int n, int k, int w, vector<string> param) {
    _n = n;
    _k = k;
    _w = w;
//    _opt = opt;

    // param[0]: d
    _d = atoi(param[0].c_str());

    _m = _n - _k;
    _q = _d - _k + 1;

    if ((_k+_m) % _q) {
        _nu = _q - (_k+_m)%_q;
    } else {
        _nu = 0;
    }

    if (_nu != 0) { 
        //cout << "The configuration is not applicable in OpenEC!!!!!!!!!!!!!!!!!!!!!" << endl;
        cout << "Add shorten idx!" << endl;
    }

    // mds
    _mds_k = _k+_nu;
    _mds_m = _m;

    // pft
    _pft_k = 2;
    _pft_m = 2;

    _t = (_k+_m+_nu)/_q;
    _sub_chunk_no = pow_int(_q, _t);

    if (w != _sub_chunk_no)
        _w = _sub_chunk_no;

    // prepare shortenidx to realidx
    for (int i=0; i<_k; i++) {
        for (int j=0; j<_w; j++) {
            int shortSubPktIdx = i * _w + j;
            _short2real[shortSubPktIdx] = shortSubPktIdx;
            _real2short[shortSubPktIdx] = shortSubPktIdx;

            int virSubPktIdx = shortSubPktIdx + (_n+_nu) * _w;
            _short2real[virSubPktIdx] = virSubPktIdx;
            _real2short[virSubPktIdx] = virSubPktIdx;
        }
    }
    for (int i=0; i<_nu; i++) {
        int shortPktIdx = _k+i;
        int realPktIdx = _n+i;
        for (int j=0; j<_w; j++) {
            int shortSubPktIdx = shortPktIdx * _w + j;
            int realSubPktIdx = realPktIdx * _w + j;
            _short2real[shortSubPktIdx] = realSubPktIdx;
            _real2short[realSubPktIdx] = shortSubPktIdx;

            int virShortSubPktIdx = shortSubPktIdx + (_n+_nu) * _w;
            int virRealSubPktIdx = realSubPktIdx + (_n+_nu) * _w;
            _short2real[virShortSubPktIdx] = virRealSubPktIdx;
            _real2short[virRealSubPktIdx] = virShortSubPktIdx;
        }
    }
    for (int i=0; i<(_n-_k); i++) {
        int shortPktIdx = _k+_nu+i;
        int realPktIdx = _k+i;
        for (int j=0; j<_w; j++) {
            int shortSubPktIdx = shortPktIdx * _w + j;
            int realSubPktIdx = realPktIdx * _w + j; 

            _short2real[shortSubPktIdx] = realSubPktIdx;
            _real2short[realSubPktIdx] = shortSubPktIdx;

            int virShortSubPktIdx = shortSubPktIdx + (_n+_nu) * _w;
            int virRealSubPktIdx = realSubPktIdx + (_n+_nu) * _w;
            _short2real[virShortSubPktIdx] = virRealSubPktIdx;
            _real2short[virRealSubPktIdx] = virShortSubPktIdx;
        }
    }

    cout << "Clay::Clay() k:" << _k << ", m:" << _m << ", d:" << _d << ", q:" << _q << ", t: " << _t << ", subchunkno:"<<_sub_chunk_no << endl;
    cout << "Clay::Clay() mds_k:" <<_mds_k << ", mds_m:" << _mds_m << endl;
    cout << "Clay::Clay() pft_k:" <<_pft_k << ", pft_m:" << _pft_m << endl;

}

int Clay::pow_int(int a, int x) {
    int power = 1;
    while (x) {
        if (x & 1) power *= a;
        x /= 2;
        a *= a;
    }
    return power;
}

void Clay::get_erasure_coordinates(vector<int> erased_chunk,
        Erasure_t** erasures) {
    int eclen=erased_chunk.size();
    for (int idx=0; idx<eclen; idx++) {
        int i=erased_chunk[idx];
        int x = i%_q;
        int y = i/_q;
        erasures[idx] = new Erasure_t(x, y);
    }
}

void Clay::get_weight_vector(Erasure_t** erasures,
        int* weight_vec) {
    int i;
    memset(weight_vec, 0, sizeof(int)*_t);
    for (i=0; i<_m; i++) {
        weight_vec[erasures[i]->_y]++;
    }
}

int Clay::get_hamming_weight(int* weight_vec) {
    int i;
    int weight=0;
    for (i=0; i<_t; i++) {
        if (weight_vec[i] != 0) weight++;
    }
    return weight;
}

void Clay::set_planes_sequential_decoding_order(int* order, Erasure_t** erasures) {
    int z, i;
    int z_vec[_t];
    for (z=0; z<_sub_chunk_no; z++) {
        get_plane_vector(z, z_vec);
        order[z] = 0;
        for (i=0; i<_m; i++) {
            if (erasures[i]->_x == z_vec[erasures[i]->_y]) {
                order[z] = order[z]+1;
            }
        }
    }
}

void Clay::get_plane_vector(int z, int* z_vec) {
    int i;
    for (i=0; i<_t; i++) {
        z_vec[_t-1-i] = z % _q;
        z = (z - z_vec[_t-1-i])/_q;
    }
}

ECDAG* Clay::Encode() {
    ECDAG* ecdag = new ECDAG();

    vector<int> erased_chunks;
    
    // xiaolu comment
    for (int idx=_k+_nu; idx<_n+_nu; idx++) {
        erased_chunks.push_back(idx);
    }

    int i;
    int x, y;
    int hm_w;
    int z, node_xy, node_sw;

    int num_erasures = erased_chunks.size();
    assert(num_erasures > 0);
    assert(num_erasures == _m);

    Erasure_t** erasures = (Erasure_t**)calloc(num_erasures, sizeof(Erasure_t*));
    int weight_vec[_t];

    // get the (x, y) coordinate of a node_xy
    get_erasure_coordinates(erased_chunks, erasures);
    cout << "Clay::decode_layered() erasures:" << endl;
    for (int i=0; i<num_erasures; i++) {
        cout << " ";
        erasures[i]->dump();
    }

    // get the number of failures in each y
    get_weight_vector(erasures, weight_vec);

    cout << "Clay::decode_layered() weight_vector: " ;
    for (int i=0; i<_t; i++) cout << weight_vec[i] << " ";
    cout << endl;

    // get the number of non-zero bit in the weight vector
    int max_weight = get_hamming_weight(weight_vec);
    cout << "Clay::decode_layered() max_weight: " << max_weight << endl;

    // the number of unpaired symbols in each plane
    int order[_sub_chunk_no];
    int z_vec[_t];
    
    // in each plane, get the number of symbols that are unpaired from the node that we should
    // encode
    set_planes_sequential_decoding_order(order, erasures);

    cout << "Clay::decode_layer() order: ";
    for (int j=0; j<_sub_chunk_no; j++) cout << order[j] << " ";
    cout << endl;

    for (hm_w = 0; hm_w <= max_weight; hm_w++) {
        for (z = 0; z < _sub_chunk_no; z++) {
            if (order[z]==hm_w) {
                // for available nodes, we get uncoupled data from coupled data
                // only used in encode_chunks
                decode_erasures(erased_chunks, z, ecdag);
            }
        }

        // from uncoupled to coupled data
        for (z=0; z<_sub_chunk_no; z++) {
            if (order[z] == hm_w) {
                get_plane_vector(z,z_vec);
                for (i = 0; i<num_erasures; i++) {
                    x = erasures[i]->_x;
                    y = erasures[i]->_y;
                    node_xy = y*_q+x;
                    node_sw = y*_q+z_vec[y];

                    if (z_vec[y] == x) {
                        int uncoupled_idx = node_xy * _w + z + (_n+_nu) * _w;
                        int coupled_idx = node_xy * _w + z;
                        //ecdag->Join(coupled_idx, {uncoupled_idx}, {1});
                        
                        int real_coupled_idx = get_real_from_short(coupled_idx);
                        int real_uncoupled_idx = get_real_from_short(uncoupled_idx);

//                        cout << "239::old: Join(" << coupled_idx << "; [" << uncoupled_idx << "])" << endl;
                        ecdag->Join(real_coupled_idx, {real_uncoupled_idx}, {1});
                    
                    } else if (z_vec[y] < x) {
                        get_coupled_from_uncoupled(x, y, z, z_vec, ecdag);
                    }
                }
            }
        }
    }

    return ecdag;
}

void Clay::decode_erasures(vector<int> erased_chunks, int z, ECDAG* ecdag) {
    int x, y;
    int node_xy, node_sw;
    int z_vec[_t];

    get_plane_vector(z,z_vec);

    for (x=0; x < _q; x++) {
        for (y=0; y < _t; y++) {
            node_xy = _q*y+x;
            node_sw = _q*y+z_vec[y];

            if (find(erased_chunks.begin(), erased_chunks.end(), node_xy) == erased_chunks.end()) {
                if (z_vec[y] < x) {
                    // this symbol should be paired
                    get_uncoupled_from_coupled(x, y, z, z_vec, ecdag);
                } else if (z_vec[y] == x) {
                    // we color this symbol, directly copy content from coupled buffers
                    int coupled_idx = node_xy * _w + z;
                    int uncoupled_idx = coupled_idx + (_n+_nu) * _w;
                    //ecdag->Join(uncoupled_idx, {coupled_idx}, {1});

                    int real_coupled_idx = get_real_from_short(coupled_idx);
                    int real_uncoupled_idx = get_real_from_short(uncoupled_idx);

//                    cout << "278::old: Join(" << uncoupled_idx << "; [" << coupled_idx << "])" << endl;
                    ecdag->Join(real_uncoupled_idx, {real_coupled_idx}, {1});
                }
            }
        }
    }
    decode_uncoupled(erased_chunks, z, ecdag);
}

vector<int> Clay::get_uncoupled_from_coupled(int x, int y, int z, int* z_vec, ECDAG* ecdag) {

    vector<int> toret;

    // the coupled index are 0, 1
    // the uncoupled index are 2, 3
    // 0, 2 refer to the node where z[y] < x


    memset(_pft_encode_matrix, 0, CLAYPFT_N_MAX * CLAYPFT_N_MAX * sizeof(int));
    generate_matrix(_pft_encode_matrix, _pft_k+_pft_m, _pft_k, 8);

    int node_xy = y*_q+x;
    int z_xy = z;
    assert(z_vec[y] < x);

    int node_sw = y*_q+z_vec[y];
    int z_sw = z + (x - z_vec[y]) * pow_int(_q,_t-1-y);

    vector<int> data;
    vector<int> code;
    
    int idx0 = node_xy * _w + z_xy;
    int idx1 = node_sw * _w + z_sw;
    int idx2 = idx0 + (_n+_nu) * _w;
    int idx3 = idx1 + (_n+_nu) * _w;

    data.push_back(idx0);
    data.push_back(idx1);
    code.push_back(idx2);
    code.push_back(idx3);

    // calculate idx2
    vector<int> coef2;
    for (int i=0; i<2; i++) {
        int curcoef = _pft_encode_matrix[_pft_k*_pft_k+i];
        coef2.push_back(curcoef);
    }
    //ecdag->Join(idx2, data, coef2);

    int real_idx2 = get_real_from_short(idx2);
    vector<int> realdata;
    for (auto tmpidx: data) {
        int tmprealidx = get_real_from_short(tmpidx);
        realdata.push_back(tmprealidx);
    }

//    cout << "333::old: Join(" << idx2 << "; [" << data[0] << ", " << data[1] << "])" << endl;
    ecdag->Join(real_idx2, realdata, coef2);
    toret.push_back(real_idx2);

    // calculate idx3
    vector<int> coef3;
    for (int i=0; i<2; i++) {
        int curcoef = _pft_encode_matrix[(_pft_k+1)*_pft_k+i];
        coef3.push_back(curcoef);
    }
    //ecdag->Join(idx3, data, coef3);

    int real_idx3 = get_real_from_short(idx3);
//    cout << "345::old: Join(" << idx3 << "; [" << data[0] << ", " << data[1] << "])" << endl;
    ecdag->Join(real_idx3, realdata, coef3);
    toret.push_back(real_idx3);

    //ecdag->BindX({idx2, idx3});
    //

    return toret;
}

void Clay::get_coupled_from_uncoupled(int x, int y, int z, int* z_vec, ECDAG* ecdag) {

    // the coupled index are 0, 1
    // the uncoupled index are 2, 3
    // 0, 2 refer to the node where z[y] < x

    memset(_pft_encode_matrix, 0, CLAYPFT_N_MAX * CLAYPFT_N_MAX * sizeof(int));
    generate_matrix(_pft_encode_matrix, _pft_k+_pft_m, _pft_k, 8);

    int node_xy = y*_q+x;
    int z_xy = z;
    assert(z_vec[y] < x);

    int node_sw = y*_q+z_vec[y]; 
    int z_sw = z + (x - z_vec[y]) * pow_int(_q,_t-1-y);

    vector<int> data;
    vector<int> code; 

    int idx0 = node_xy * _w + z_xy;
    int idx1 = node_sw * _w + z_sw;
    int idx2 = idx0 + (_n+_nu) * _w;
    int idx3 = idx1 + (_n+_nu) * _w;

    data.push_back(idx2);
    data.push_back(idx3);

    vector<int> select_lines = {2, 3};
    int _select_matrix[_pft_k*_pft_k];
    for (int i=0; i<_pft_k; i++) {
        int sidx = select_lines[i];
        memcpy(_select_matrix + i * _pft_k,
                _pft_encode_matrix + sidx * _pft_k,
                sizeof(int) * _pft_k);
    }

    int _invert_matrix[_pft_k*_pft_k];
    jerasure_invert_matrix(_select_matrix, _invert_matrix, _pft_k, 8);

    // calculate idx0
    vector<int> coef0;
    for (int i=0; i<2; i++) {
        int curcoef = _invert_matrix[i];
        coef0.push_back(curcoef);
    }
    //ecdag->Join(idx0, data, coef0);

    int real_idx0 = get_real_from_short(idx0);
    vector<int> realdata;
    for (auto tmpidx: data) {
        int tmprealidx = get_real_from_short(tmpidx);
        realdata.push_back(tmprealidx);
    }

//    cout << "405::old: Join(" << idx0 << "; [" << data[0] << ", " << data[1] << "])" << endl;
    ecdag->Join(real_idx0, realdata, coef0);

    // calculate idx1
    vector<int> coef1;
    for (int i=0; i<2; i++) {
        int curcoef = _invert_matrix[2+i];
        coef1.push_back(curcoef);
    }
    //ecdag->Join(idx1, data, coef1);

    int real_idx1 = get_real_from_short(idx1);
//    cout << "417::old: Join(" << idx1 << "; [" << data[0] << ", " << data[1] << "])" << endl;
    ecdag->Join(real_idx1, realdata, coef1);

    //ecdag->BindX({idx0, idx1});
}

vector<int> Clay::get_coupled1_from_pair02(int x, int y, int z, int* z_vec, ECDAG* ecdag) {

    vector<int> toret;
    // the coupled index are 0, 1
    // the uncoupled index are 2, 3
    // 0, 2 refer to the node where z[y] < x
    // we know 0, 2 and want to repair 1


    memset(_pft_encode_matrix, 0, CLAYPFT_N_MAX * CLAYPFT_N_MAX * sizeof(int));
    generate_matrix(_pft_encode_matrix, _pft_k+_pft_m, _pft_k, 8);

    // node_xy refers to 0, 2
    int node_xy = y*_q + x;
    int z_xy = z;
    assert(z_vec[y] < x);

    int node_sw = y*_q+z_vec[y];
    int z_sw = z + (x - z_vec[y]) * pow_int(_q,_t-1-y);

    vector<int> data; 
    vector<int> code;

    int idx0 = node_xy * _w + z_xy;
    int idx1 = node_sw * _w + z_sw;
    int idx2 = idx0 + (_n+_nu) * _w;
    int idx3 = idx1 + (_n+_nu) * _w;

    data.push_back(idx0);
    data.push_back(idx2);

    vector<int> select_lines = {0,2};
    int _select_matrix[_pft_k*_pft_k];
    for (int i=0; i<_pft_k; i++) {
        int sidx = select_lines[i];
        memcpy(_select_matrix + i * _pft_k,
                _pft_encode_matrix + sidx * _pft_k,
                sizeof(int) * _pft_k);
    }

    int _invert_matrix[_pft_k*_pft_k];
    jerasure_invert_matrix(_select_matrix, _invert_matrix, _pft_k, 8);

    int _select_vector[_pft_k];
    memcpy(_select_vector,
            _pft_encode_matrix + 1 * _pft_k,
            _pft_k * sizeof(int));
    int* _coef_vector = jerasure_matrix_multiply(
            _select_vector, _invert_matrix, 1, _pft_k, _pft_k, _pft_k, 8);

    // calculate idx1
    vector<int> coef;
    for (int i=0; i<_pft_k; i++) coef.push_back(_coef_vector[i]);
    //ecdag->Join(idx1, data, coef);

    int real_idx1 = get_real_from_short(idx1);
    vector<int> realdata;
    for (auto tmpidx: data) {
        int tmprealidx = get_real_from_short(tmpidx);
        realdata.push_back(tmprealidx);
    }

//    cout << "484::old: Join(" << idx1 << "; [" << data[0] << ", " << data[1] << "])" << endl;
    ecdag->Join(real_idx1, realdata, coef);
    toret.push_back(real_idx1);
    return toret;
}

vector<int> Clay::get_coupled0_from_pair13(int x, int y, int z, int* z_vec, ECDAG* ecdag) {

    vector<int> toret;
    // the coupled index are 0, 1
    // the uncoupled index are 2, 3
    // 0, 2 refer to the node where z[y] < x
    // we know 1, 3 and want to repair 0

    memset(_pft_encode_matrix, 0, CLAYPFT_N_MAX * CLAYPFT_N_MAX * sizeof(int));
    generate_matrix(_pft_encode_matrix, _pft_k+_pft_m, _pft_k, 8);

    // node_xy refers to 1, 3
    int node_xy = y*_q + x;
    int z_xy = z;
    assert(z_vec[y] > x);

    int node_sw = y*_q+z_vec[y];
    int z_sw = z + (x - z_vec[y]) * pow_int(_q,_t-1-y);

    vector<int> data; 
    vector<int> code;

    int idx0 = node_sw * _w + z_sw;
    int idx1 = node_xy * _w + z_xy;
    int idx2 = idx0 + (_n+_nu) * _w;
    int idx3 = idx1 + (_n+_nu) * _w;

    data.push_back(idx1);
    data.push_back(idx3);

    vector<int> select_lines = {1,3};
    int _select_matrix[_pft_k*_pft_k];
    for (int i=0; i<_pft_k; i++) {
        int sidx = select_lines[i];
        memcpy(_select_matrix + i * _pft_k,
                _pft_encode_matrix + sidx * _pft_k,
                sizeof(int) * _pft_k);
    }

    int _invert_matrix[_pft_k*_pft_k];
    jerasure_invert_matrix(_select_matrix, _invert_matrix, _pft_k, 8);

    int _select_vector[_pft_k];
    memcpy(_select_vector,
            _pft_encode_matrix + 0 * _pft_k,
            _pft_k * sizeof(int));
    int* _coef_vector = jerasure_matrix_multiply(
            _select_vector, _invert_matrix, 1, _pft_k, _pft_k, _pft_k, 8);

    // calculate idx1
    vector<int> coef;
    for (int i=0; i<_pft_k; i++) coef.push_back(_coef_vector[i]);
    //ecdag->Join(idx0, data, coef);

    int real_idx0 = get_real_from_short(idx0);
    vector<int> realdata;
    for (auto tmpidx: data) {
        int tmprealidx = get_real_from_short(tmpidx);
        realdata.push_back(tmprealidx);
    }

//    cout << "548::old: Join(" << idx0 << "; [" << data[0] << ", " << data[1] << "])" << endl;
    ecdag->Join(real_idx0, realdata, coef);
    toret.push_back(real_idx0);
    return toret;
}

vector<int> Clay::decode_uncoupled(vector<int> erased_chunks, int z, ECDAG* ecdag) {
    vector<int> toret;
    // in the layer z, we focus on the uncoupled data
    // we need to repair chunks in erased_chunks from _mds_k chunks

    memset(_mds_encode_matrix, 0, CLAYMDS_N_MAX * CLAYMDS_N_MAX * sizeof(int));
    generate_matrix(_mds_encode_matrix, _mds_k+_mds_m, _mds_k, 8);

    // figure out the select lines
    vector<int> select_lines;
    vector<int> data;
    for (int i=0; i<_mds_k+_mds_m; i++) {
        if (find(erased_chunks.begin(), erased_chunks.end(), i) == erased_chunks.end()) {
            select_lines.push_back(i);
            int curidx = i * _w + z + (_n+_nu) * _w;
            data.push_back(curidx);
        }

        if (select_lines.size() == _mds_k)
            break;
    }

    int _select_matrix[_mds_k*_mds_k];
    for (int i=0; i<_mds_k; i++) {
        int sidx = select_lines[i];
        memcpy(_select_matrix + i * _mds_k,
                _mds_encode_matrix + sidx * _mds_k,
                sizeof(int) * _mds_k); 
    }

    int _invert_matrix[_mds_k*_mds_k];
    jerasure_invert_matrix(_select_matrix, _invert_matrix, _mds_k, 8); 

    vector<int> tobind;
    for (int i=0; i<erased_chunks.size(); i++) {
        int cidx = erased_chunks[i];
        int _select_vector[_mds_k];
        memcpy(_select_vector,
                _mds_encode_matrix + cidx * _mds_k,
                _mds_k * sizeof(int));
        int* _coef_vector = jerasure_matrix_multiply(
                _select_vector, _invert_matrix, 1, _mds_k, _mds_k, _mds_k, 8);
        vector<int> coef;
        for (int i=0; i<_mds_k; i++) coef.push_back(_coef_vector[i]);
        int targetidx = cidx * _w + z + (_n+_nu) * _w;
        //ecdag->Join(targetidx, data, coef);

        int real_targetidx = get_real_from_short(targetidx);
        vector<int> realdata;
        for (auto tmpidx: data) {
            int tmprealidx = get_real_from_short(tmpidx);
            realdata.push_back(tmprealidx);
        }

//        cout << "605::old: Join(" << targetidx << "; [" << data[0] << ", " << data[1] << "])" << endl;
        ecdag->Join(real_targetidx, realdata, coef);
        toret.push_back(real_targetidx);
    }

    return toret; 
}

int Clay::is_repair(vector<int> want_to_read, vector<int> avail) {
    if (want_to_read.size() > 1) return 0;
    if ((int)avail.size() < _d) return 0;
    return 1;
}

void Clay::minimum_to_repair(vector<int> want_to_read, 
        vector<int> available_chunks, 
        unordered_map<int, vector<pair<int, int>>>& minimum) {
    
    assert(want_to_read.size() == 1);
    int i = want_to_read[0];
    int lost_node_index = (i < _k) ? i : i+_nu;
    int rep_node_index = 0;

    // add all the nodes in lost node's y column.
    vector<pair<int, int>> sub_chunk_ind;
    get_repair_subchunks(lost_node_index, sub_chunk_ind);

    if ((available_chunks.size() >= _d)) {
        for (int j = 0; j < _q; j++) {
            if (j != lost_node_index%_q) {
                rep_node_index = (lost_node_index/_q)*_q+j;
                if (rep_node_index < _k) {
                    minimum.insert(make_pair(rep_node_index, sub_chunk_ind));
                } else if (rep_node_index >= _k+_nu) {
                    minimum.insert(make_pair(rep_node_index-_nu, sub_chunk_ind));
                }
            }
        }

        for (auto chunk : available_chunks) {
            if (minimum.size() >= _d) {
                break;
            }
            if (minimum.find(chunk) == minimum.end()) {
                minimum.emplace(chunk, sub_chunk_ind);
            }
        }
    }
}

void Clay::get_repair_subchunks(int lost_node, vector<pair<int, int>> &repair_sub_chunks_ind) {
    int y_lost = lost_node / _q;
    int x_lost = lost_node % _q;
    int seq_sc_count = pow_int(_q, _t-1-y_lost);
    int num_seq = pow_int(_q, y_lost);
    int index = x_lost * seq_sc_count;
    for (int ind_seq = 0; ind_seq < num_seq; ind_seq++) {
        repair_sub_chunks_ind.push_back(make_pair(index, seq_sc_count));
        index += _q * seq_sc_count;
    }
}

int Clay::get_repair_sub_chunk_count(vector<int> want_to_read) {
    // get the number of sub chunks we should repair???
    int repair_subchunks_count = 1;
    // record the number of failures in each y
    int weight_vector[_t];
    memset(weight_vector, 0, _t*sizeof(int));
    for (auto i: want_to_read) {
        weight_vector[i/_q]++;
    }
    for (int y = 0; y < _t; y++) repair_subchunks_count = repair_subchunks_count*(_q-weight_vector[y]);
    return _sub_chunk_no - repair_subchunks_count;
}

ECDAG* Clay::Decode(vector<int> from, vector<int> to) {
    cout << "Clay::Decode" << endl;
    ECDAG* ecdag = new ECDAG();

    int lost = to[0] / _w;

    vector<int> want_to_read={lost};
    cout << "Decode: lost = " << lost << endl;

    vector<int> avail;
    for (int i=0; i<(_k+_nu+_m); i++) {
        if (i == lost) continue;
        avail.push_back(i);
    }

    assert(is_repair(want_to_read, avail) == 1);

    // minimum map records symbols we should read in each node <startid, succeed number>
    unordered_map<int, vector<pair<int, int>>> minimum_map;
    minimum_to_repair(want_to_read, avail, minimum_map);

    unordered_map<int, bool> chunks;
    for (auto item: minimum_map) {
        int nodeid=item.first;
        chunks.insert(make_pair(nodeid, true));
    }

    assert(want_to_read.size() == 1);
    assert(minimum_map.size() == _d);

    int repair_sub_chunk_no = get_repair_sub_chunk_count(want_to_read);
    
    vector<pair<int, int>> repair_sub_chunks_ind;
    
    unordered_map<int, bool> recovered_data;
    unordered_map<int, bool> helper_data;
    vector<int> aloof_nodes;

    for (int i =  0; i < _k + _m; i++) {
        if (chunks.find(i) != chunks.end()) {
            // i is a helper
            if (i<_k) {
                helper_data.insert(make_pair(i, true));
            } else {
                helper_data.insert(make_pair(i+_nu, true));
            }
        } else {
            if (find(want_to_read.begin(), want_to_read.end(), i) == want_to_read.end()) {
                int aloof_node_id = (i < _k) ? i: i+_nu;
                aloof_nodes.push_back(aloof_node_id);
            } else {
                int lost_node_id = (i < _k) ? i : i+_nu;
                recovered_data.insert(make_pair(lost_node_id, true));
                get_repair_subchunks(lost_node_id, repair_sub_chunks_ind);
            }
        }
    }

    // this is for shortened codes i.e., when nu > 0
    for (int i=_k; i < _k+_nu; i++) {
        helper_data.insert(make_pair(i, true));
    }

    assert(helper_data.size() + aloof_nodes.size() + recovered_data.size() == _q*_t);

    repair_one_lost_chunk(recovered_data, aloof_nodes,
            helper_data, repair_sub_chunks_ind, ecdag);

    return ecdag;
}

void Clay::repair_one_lost_chunk(unordered_map<int, bool>& recovered_data,
        vector<int>& aloof_nodes,
        unordered_map<int, bool>& helper_data,
        vector<pair<int,int>> &repair_sub_chunks_ind,
        ECDAG* ecdag) {

    cout << "recovered_data: ";
    for (auto item: recovered_data)
        cout << item.first << " ";
    cout << endl;

    cout << "aloof_nodes: ";
    for (auto item: aloof_nodes)
        cout << item << " ";
    cout << endl;

    cout << "helper_data: ";
    for (auto item: helper_data)
        cout << item.first << " ";
    cout << endl;

    cout << "repair_sub_chunks_ind: ";
    for (auto item: repair_sub_chunks_ind) {
        cout << "(" << item.first << ", " << item.second << ") ";
    }
    cout << endl;

    int repair_subchunks = _sub_chunk_no / _q;
    int z_vec[_t];
    unordered_map<int, vector<int>> ordered_planes;
    unordered_map<int, int> repair_plane_to_ind;

    int count_retrieved_sub_chunks = 0;
    int plane_ind = 0;

    for (auto item: repair_sub_chunks_ind) {
        int index = item.first;
        int count = item.second;

        for (int j = index; j < index + count; j++) {
            get_plane_vector(j, z_vec);

            int order = 0;
            // check across all erasures and aloof nodes
            // check recover node the number of vertex colored red starting from the index
            for (auto ritem: recovered_data) {
                int node = ritem.first;
                if (node % _q == z_vec[node / _q]) order++;
            }
            for (auto node : aloof_nodes) {
                if (node % _q == z_vec[node / _q]) order++;
            }
            assert(order > 0);
            // order means the number of red vertex in the plane j among all recover node and aloof
            // node
            if (ordered_planes.find(order) == ordered_planes.end()) {
                vector<int> curlist;
                curlist.push_back(j);
                ordered_planes.insert(make_pair(order, curlist));
            } else {
                ordered_planes[order].push_back(j);
            }
            repair_plane_to_ind[j] = plane_ind;
            plane_ind++;
        }
    }

    assert(plane_ind == repair_subchunks);

    int plane_count = 0;
    int lost_chunk;
    int count = 0;
    for (auto item: recovered_data) {
        lost_chunk = item.first;
        count++;
    }
    assert(count == 1);

    // add all nodes in the same y of the lost chunk into erasures, as well as the aloof node
    vector<int> erasures;
    for (int i = 0; i < _q; i++) {
        erasures.push_back(lost_chunk - lost_chunk % _q + i);
    }
    for (auto node : aloof_nodes) {
        erasures.push_back(node);
    }

    vector<int> orderlist;
    for (auto item: ordered_planes)  {
        int order = item.first;
        orderlist.push_back(order);
    }
    sort(orderlist.begin(), orderlist.end());

    vector<int> list1;
    vector<int> list2;
    vector<int> list3;

    // we start from the planes that have the minimum number of red vertex
    for (int order: orderlist) {
        plane_count += ordered_planes[order].size();

        // calculate uncoupled value from coupled value read from disk
        for (auto z : ordered_planes[order]) {
            get_plane_vector(z, z_vec);

            for (int y = 0; y < _t; y++) {
                for (int x = 0; x < _q; x++) {
                    int node_xy = y*_q + x;
                    if (find(erasures.begin(), erasures.end(), node_xy) == erasures.end()) {
                        // current node_xy does not erasure
                        assert(helper_data.find(node_xy) != helper_data.end());

                        int z_sw = z + (x - z_vec[y])*pow_int(_q,_t-1-y);
                        int node_sw = y*_q + z_vec[y];
                        if (z_vec[y] == x) {
                            // red vertex
                            int coupled_idx = node_xy * _w + z;
                            int uncoupled_idx = coupled_idx + (_n+_nu) * _w;
                            //ecdag->Join(uncoupled_idx, {coupled_idx}, {1});

                            int real_coupled_idx = get_real_from_short(coupled_idx);
                            int real_uncoupled_idx = get_real_from_short(uncoupled_idx);

//                            cout << "878::old: Join(" << uncoupled_idx << "; [" << coupled_idx << "])" << endl;
                            ecdag->Join(real_uncoupled_idx, {real_coupled_idx}, {1});
                            list1.push_back(real_uncoupled_idx);
                        } else if (z_vec[y] < x) {
                            vector<int> tmplist = get_uncoupled_from_coupled(x, y, z, z_vec, ecdag);
                            for (auto item: tmplist)
                                list1.push_back(item);
                        }
                    }
                }
            }

            assert(erasures.size() <= _m);
            vector<int> tmplist = decode_uncoupled(erasures, z, ecdag);
            for (auto item: tmplist)
                list2.push_back(item);

            // check node in erasures
            // if the node is recovery node, the vertex is colored red, repair coupled from
            // uncoupled
            // if the node is not recovery node, pair the vertex with the one in the recovery node
            // and repair the paired

            for (auto i : erasures) {
                int x = i % _q;
                int y = i / _q;

                int node_sw = y*_q+z_vec[y];
                int z_sw = z + (x - z_vec[y]) * pow_int(_q,_t-1-y);

                if (i == lost_chunk) {
                    assert(z_vec[y] == x);

                    // red vertex, we directly get coupled data from uncoupled data
                    int coupled_idx = i * _w + z;
                    int uncoupled_idx = coupled_idx + (_n+_nu) * _w;
                    //ecdag->Join(coupled_idx, {uncoupled_idx}, {1});
                    
                    int real_coupled_idx = get_real_from_short(coupled_idx);
                    int real_uncoupled_idx = get_real_from_short(uncoupled_idx);
                    
//                    cout << "914::old: Join(" << coupled_idx << "; [" << uncoupled_idx << "])" << endl;
                    ecdag->Join(real_coupled_idx, {real_uncoupled_idx}, {1});
                    list3.push_back(real_coupled_idx);
                } else {
                    if (find(aloof_nodes.begin(), aloof_nodes.end(), i) != aloof_nodes.end())
                        continue;

                    if (z_vec[y] < x) {
                        if (node_sw == lost_chunk) {
                            vector<int> tmplist = get_coupled1_from_pair02(x, y, z, z_vec, ecdag);
                            for (auto item: tmplist)
                                list3.push_back(item);
                        }
                    } else if (z_vec[y] > x) {
                        if (node_sw == lost_chunk) {
                            vector<int> tmplist = get_coupled0_from_pair13(x, y, z, z_vec, ecdag);
                            for (auto item: tmplist)
                                list3.push_back(item);
                        }
                    }
                }
            }
        }
    }

    //cout << "list1: ";
    //for (auto item: list1)
    //    cout << item << " ";
    //cout << endl;
    //cout << "list2: ";
    //for (auto item: list2)
    //    cout << item << " ";
    //cout << endl;
    //cout << "list3: ";
    //for (auto item: list3)
    //    cout << item << " ";
    //cout << endl;

//    int vidx1 = ecdag->BindX(list1);
//    int vidx2 = ecdag->BindX(list2);
//    ecdag->BindY(vidx2, list1[0]);
//    int vidx3 = ecdag->BindX(list3);
//    ecdag->BindY(vidx3, list2[0]);
}

void Clay::generate_matrix(int* matrix, int rows, int cols, int w) {
    int k = cols;
    int n = rows;
    int m = n - k;

    memset(matrix, 0, rows * cols * sizeof(int));
    for(int i=0; i<k; i++) {
        matrix[i*k+i] = 1; 
    }

    int a = 1;
    for (int i=0; i<m; i++) {
        int tmp = 1;
        for (int j=0; j<k; j++) {
            matrix[(i+k)*cols+j] = tmp;
            tmp = Computation::singleMulti(tmp, a, w);
        }
        a = a + 1;
    }
}

//void Clay::print_matrix(int* matrix, int row, int col) {
//    for (int i=0; i<row; i++) {
//        for (int j=0; j<col; j++) {
//            cout << matrix[i*col+j] << " ";
//        }
//        cout << endl;
//    }
//}
//
//void Clay::Place(vector<vector<int>>& group) {
//
//}
//
//void Clay::Shorten(unordered_map<int, int>& shortening) {
//    // map shortenidx 2 storageidx
//    for (int i=0; i<_k; i++)
//        shortening.insert(make_pair(i, i));
//    for (int i=_k; i<_k+_nu; i++)
//        shortening.insert(make_pair(i, -1));
//    for (int i=_k+_nu; i<_n+_nu; i++)
//        shortening.insert(make_pair(i, i-_nu));
//}

int Clay::get_real_from_short(int idx) {
    if (_short2real.find(idx) == _short2real.end())
        return idx;
    else
        return _short2real[idx];
}

int Clay::get_short_from_real(int idx) {
    if (_real2short.find(idx) == _real2short.end())
        return idx;
    else
        return _real2short[idx];
}
