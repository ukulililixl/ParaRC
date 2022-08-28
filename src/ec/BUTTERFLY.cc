#include "BUTTERFLY.hh"

BUTTERFLY::BUTTERFLY(int n, int k, int w, vector<string> param) {
//BUTTERFLY::BUTTERFLY(int n, int k, int w, int opt, vector<string> param) {
//  cout << "BUTTERRFL4" << endl;
  _n = n;
  _k = k;
  _w = w;
//  _opt = opt;

  _m = _n - _k;
  if (_m != 2) cout << "Error: Butterfly code: n must be k + 2." << endl;

  _chunk_num_per_node = _w;
  _sys_chunk_num = _k * _chunk_num_per_node;
  _enc_chunk_num = _m * _chunk_num_per_node;
  _total_chunk_num = _sys_chunk_num + _enc_chunk_num;

  _tmp = _total_chunk_num;
}

ECDAG* BUTTERFLY::Encode() {
  ECDAG* ecdag = new ECDAG();
  vector<int> data;
  vector<int> code;
  for (int i=0; i<_k; i++) {
    for (int j=0; j<_w; j++) data.push_back(i*_w+j);
  }
  for (int i=_k; i<_n; i++) {
    for (int j=0; j<_w; j++) code.push_back(i*_w+j);
  }

  generate_encoding_matrix();
  for (int i=0; i<_enc_chunk_num; i++) {
    vector<int> coef;
    for (int j=0; j<_sys_chunk_num; j++) {
      coef.push_back(_enc_matrix[(_sys_chunk_num+i) * _sys_chunk_num + j]);
    }
    ecdag->Join(code[i], data, coef);
  }
//  ecdag->BindX(code);

  return ecdag;
}

void BUTTERFLY::print_double_vec(vector<vector<int>> off){
  for (int i = 0; i < off.size(); i++){
    for (int j = 0; j < off[i].size(); j++){
      cout << off[i][j] << " ";
    }
    cout << endl;
  }
}

void BUTTERFLY::generate_xor_data_matrix(int *xor_mat, int sid){
  vector<vector<int>> init_mat;
  for (int i = 0; i < _w; i++){
    vector<int> row_list;
    for (int j = 0; j < _n; j++){
      row_list.push_back(j * _w + i);
    }
    init_mat.push_back(row_list);
  }

  vector<vector<int>> enc_vec = butterfly_func(init_mat, _w, _k);

  vector<vector<int>> xor_vec;
  for (int i = _w - 1; i >=  _w / 2; i --){
    vector<int> row_list;
    for (int j = 0; j < enc_vec[i].size(); j ++){
      if (enc_vec[i][j] / _w == sid) {
        row_list.push_back(enc_vec[i][j] % _w);
      }
    }
    xor_vec.push_back(row_list);
  }

  for (int i = 0; i < _w / 2 * _w; i++){
    xor_mat[i] = 0;
  }

  for (int i = 0; i < xor_vec.size(); i++){
    for (int j = 0; j < xor_vec[i].size(); j++){
      xor_mat[i * _w + xor_vec[i][j]] = 1;
    }
  }
}

// situation (1): data column d0 - dk-2
void BUTTERFLY::generate_butterfly_parity_decoding_matrix_1(int *parity_dec, int rBlkIdx, vector<int> off) {
  // step 1: init a data matrix(w * k) and repair matrix
  vector<vector<int>> init_mat;
  vector<vector<int>> repair_mat, H, B;
  for (int i = 0; i < _w; i++){
    vector<int> row_list;
    for (int j = 0; j < _n; j++){
      row_list.push_back(j * _w + i);
    }
    init_mat.push_back(row_list);
  }
  for (int i = 0; i < off.size(); i++){
    vector<int> row_list;
    for (int j = 0; j < _k; j++){
      if (j != rBlkIdx){
        row_list.push_back(init_mat[off[i]][j]);
      }
    }
    repair_mat.push_back(row_list);
    H.push_back({init_mat[off[i]][_k]});
    B.push_back({init_mat[off[i]][_k + 1]});
  }

  // step 2: butterfly parities function
  vector<vector<int>> enc_vec = butterfly_func(init_mat, _w, _k);
  vector<vector<int>> h = add_mat(repair_mat, H);
  vector<vector<int>> b = add_mat(butterfly_func(repair_mat, _w / 2, _k - 1), B);

  // step 3: use h and b to rebuild each lost sub-block
  vector<vector<int>> dec_mat;
  map<int,vector<int>> dec_map;
  int h_pointer, b_pointer;
  h_pointer = 0;
  b_pointer = 0;
  for (int i = 0; i < _w; i++){
    auto t = find(off.begin(),off.end(),i);
    if (t != off.end()){
      dec_map.insert({i, h[h_pointer]});
      h_pointer ++;
    }
  }
  for (int i = 0; i < off.size(); i++){
    int lost_index;
    vector<int> depend_index;
    for (int j = 0; j < enc_vec[off[i]].size(); j++){
      if (enc_vec[off[i]][j] / _w == rBlkIdx){
        auto t = find(off.begin(), off.end(), enc_vec[off[i]][j] % _w);
        if (t == off.end()){
          lost_index = enc_vec[off[i]][j] % _w;
        } else {
          depend_index.push_back(enc_vec[off[i]][j] % _w);
        }
      }
    }
    vector<int> b_index(b[b_pointer]);
    b_pointer ++;
    for (int j = 0; j < depend_index.size(); j++){
      for (int x = 0; x < dec_map[depend_index[j]].size(); x++){
        int add_number = dec_map[depend_index[j]][x];
        if (find(b_index.begin(), b_index.end(), add_number) == b_index.end()){
          b_index.push_back(add_number);
        } else {
          b_index.erase(remove(b_index.begin(), b_index.end(), add_number), b_index.end());
        }
      }
    }
    dec_map.insert({lost_index, b_index});
  }

  for (int i = 0; i < _w; i++){
    dec_mat.push_back(dec_map[i]);
  }

  // step 4: generate butterfly parity decoding matrix
  int *parity_all = new int[_w * _w * _n];
  for (int i = 0; i < _w * _w * _n; i++){
    parity_all[i] = 0;
  }
  for (int i = 0; i < _w; i++){
    for (int j = 0; j < dec_mat[i].size(); j ++){
      parity_all[i * _w * _n + dec_mat[i][j]] = 1;
    }
  }

  for (int i = 0; i < _w * (_n - 1) * _w / 2; i++){
    parity_dec[i] = 0;
  }
  int row_index;
  for (int i = 0; i < _w; i++){
    row_index = 0;
    for (int j = 0; j < _w * _n; j ++){
      if (((j / _w) != rBlkIdx) && (find(off.begin(), off.end(), (j % _w)) != off.end())){
        parity_dec[i * (_n - 1) * _w / 2 + row_index] = parity_all[i * _w * _n + j];
        row_index += 1;
      }
    }
  }
}

// situation (2): data column dk-1
void BUTTERFLY::generate_butterfly_parity_decoding_matrix_2(int *parity_dec, int rBlkIdx, vector<int> off1, vector<int> off2) {
  // step 1: init a data matrix(w * k) and repair matrix
  vector<vector<int>> init_mat;
  vector<vector<int>> repair_mat, H, B;
  for (int i = 0; i < _w; i++){
    vector<int> row_list;
    for (int j = 0; j < _n; j++){
      row_list.push_back(j * _w + i);
    }
    init_mat.push_back(row_list);
  }
  for (int i = 0; i < off2.size(); i++){
    vector<int> row_list;
    for (int j = 0; j < _k; j++){
      if (j != rBlkIdx){
        row_list.push_back(init_mat[off2[i]][j]);
      }
    }
    repair_mat.push_back(row_list);
    H.push_back({init_mat[off2[i]][_k]});
  }
  for (int i = 0; i < off1.size(); i++){
    B.push_back({init_mat[off1[i]][_k + 1]});
  }

  // step 2: butterfly parities function
  vector<vector<int>> enc_vec = butterfly_func(init_mat, _w, _k);
  vector<vector<int>> h = add_mat(repair_mat, H);
  vector<vector<int>> b = add_mat(butterfly_func(repair_mat, _w / 2, _k - 1), B);

  // step 3: use h and b to rebuild each lost sub-block
  vector<vector<int>> dec_mat;
  map<int,vector<int>> dec_map;
  int h_pointer, b_pointer;
  h_pointer = 0;
  b_pointer = 0;
  for (int i = 0; i < _w; i++){
    auto t = find(off2.begin(),off2.end(),i);
    if (t != off2.end()){
      dec_map.insert({i, h[h_pointer]});
      h_pointer ++;
    }
  }
  for (int i = 0; i < off1.size(); i++){
    int lost_index;
    vector<int> depend_index;
    for (int j = 0; j < enc_vec[off1[i]].size(); j++){
      if (enc_vec[off1[i]][j] / _w == rBlkIdx){
        auto t = find(off1.begin(), off1.end(), enc_vec[off1[i]][j] % _w);
        if (t != off1.end()){
          lost_index = enc_vec[off1[i]][j] % _w;
        } else {
          depend_index.push_back(enc_vec[off1[i]][j] % _w);
        }
      }
    }
    vector<int> b_index(b[b_pointer]);
    b_pointer ++;
    for (int j = 0; j < depend_index.size(); j++){
      for (int x = 0; x < dec_map[depend_index[j]].size(); x++){
        int add_number = dec_map[depend_index[j]][x];
        if (find(b_index.begin(), b_index.end(), add_number) == b_index.end()){
          b_index.push_back(add_number);
        } else {
          b_index.erase(remove(b_index.begin(), b_index.end(), add_number), b_index.end());
        }
      }
    }
    dec_map.insert({lost_index, b_index});
  }

  for (int i = 0; i < _w; i++){
    dec_mat.push_back(dec_map[i]);
  }

  // step 4: generate butterfly parity decoding matrix
  int *parity_all = new int[_w * _w * _n];
  for (int i = 0; i < _w * _w * _n; i++){
    parity_all[i] = 0;
  }
  for (int i = 0; i < _w; i++){
    for (int j = 0; j < dec_mat[i].size(); j ++){
      parity_all[i * _w * _n + dec_mat[i][j]] = 1;
    }
  }

  for (int i = 0; i < _w * (_n - 1) * _w / 2; i++){
    parity_dec[i] = 0;
  }
  int row_index;
  for (int i = 0; i < _w; i++){
    row_index = 0;
    for (int j = 0; j < _w * _n; j ++){
      if ((((j / _w) != rBlkIdx) && ((j / _w) != _n - 1) && (find(off2.begin(), off2.end(), (j % _w)) != off2.end())) || (((j / _w) == _n - 1) && (find(off1.begin(), off1.end(), (j % _w)) != off1.end()))){
        parity_dec[i * (_n - 1) * _w / 2 + row_index] = parity_all[i * _w * _n + j];
        row_index += 1;
      }
    }
  }
}

// situation (3): parity column H
void BUTTERFLY::generate_butterfly_parity_decoding_matrix_3(int *parity_dec, int rBlkIdx, vector<int> off) {
  // step 1: init a data matrix(w * k) and repair matrix
  vector<vector<int>> init_mat;
  vector<vector<int>> repair_mat, H, B;
  for (int i = 0; i < _w; i++){
    vector<int> row_list;
    for (int j = 0; j < _n; j++){
      row_list.push_back(j * _w + i);
    }
    init_mat.push_back(row_list);
  }
  for (int i = 0; i < off.size(); i++){
    vector<int> row_list;
    for (int j = 0; j < _k; j++){
      row_list.push_back(init_mat[off[i]][j]);
    }
    repair_mat.push_back(row_list);
    H.push_back({init_mat[off[i]][_k]});
    B.push_back({init_mat[off[i]][_k + 1]});
  }

  // step 2: butterfly parities function
  vector<vector<int>> enc_vec = butterfly_func(init_mat, _w, _k);

  // step 3: repair lower H and use lower B to repair top H
  vector<vector<int>> dec_mat;
  map<int,vector<int>> dec_map;
  int h_pointer, b_pointer;
  h_pointer = 0;
  b_pointer = 0;
  for (int i = 0; i < _w; i++){
    auto t = find(off.begin(),off.end(),i);
    if (t != off.end()){
      dec_map.insert({i, repair_mat[h_pointer]});
      h_pointer ++;
    }
  }

  for (int i = 0; i < off.size(); i++){
    int lost_index;
    vector<int> row_list;
    for (int j = 0; j < enc_vec[off[i]].size(); j ++){
      if (enc_vec[off[i]][j] % _w < _w / 2){
        lost_index = enc_vec[off[i]][j] % _w;
      } else {
        row_list.push_back(enc_vec[off[i]][j]);
      }
    }
    row_list.push_back(B[b_pointer][0]);
    b_pointer ++;
    dec_map.insert({lost_index, row_list});
  }

  for (int i = 0; i < _w; i++){
    dec_mat.push_back(dec_map[i]);
  }

  // step 4: generate butterfly parity decoding matrix
  int *parity_all = new int[_w * _w * _n];
  for (int i = 0; i < _w * _w * _n; i++){
    parity_all[i] = 0;
  }
  for (int i = 0; i < _w; i++){
    for (int j = 0; j < dec_mat[i].size(); j ++){
      parity_all[i * _w * _n + dec_mat[i][j]] = 1;
    }
  }

  for (int i = 0; i < _w * (_n - 1) * _w / 2; i++){
    parity_dec[i] = 0;
  }
  int row_index;
  for (int i = 0; i < _w; i++){
    row_index = 0;
    for (int j = 0; j < _w * _n; j ++){
      if (((j / _w) != rBlkIdx) && (find(off.begin(), off.end(), (j % _w)) != off.end())){
        parity_dec[i * (_n - 1) * _w / 2 + row_index] = parity_all[i * _w * _n + j];
        row_index += 1;
      }
    }
  }
}

// situation (4): parity column B
void BUTTERFLY::generate_butterfly_parity_decoding_matrix_4(int *parity_dec) {
  // step 1: init a data matrix(w * k) and repair matrix
  vector<vector<int>> init_mat;
  vector<vector<int>> repair_mat, H, D_0;
  for (int i = 0; i < _w / 2; i++){
    vector<int> row_list;
    for (int j = 0; j < _n; j++){
      row_list.push_back(j * _w / 2 + i);
    }
    init_mat.push_back(row_list);
  }

  for (int i = 0; i < _w / 2; i++){
    vector<int> row_list;
    for (int j = 0; j < _k; j++){
      if (j > 0){
        row_list.push_back(init_mat[i][j]);
      }
    }
    repair_mat.push_back(row_list);
    H.push_back({init_mat[i][_k]});
    D_0.push_back({init_mat[i][0]});
  }

  // step 2: repair top half B
  vector<vector<int>> B_top = add_mat(butterfly_func(repair_mat, _w / 2, _k - 1), perm_mat(H));

  // step 3: repair bottom half B
  vector<vector<int>> B_bottom = add_mat(perm_mat(D_0), perm_mat(repair_mat));

  // step 4: use B_top and B_bottom to generate matrix
  vector<vector<int>> dec_mat;
  for (int i = 0; i < B_top.size(); i ++){
    dec_mat.push_back(B_top[i]);
  }
  for (int i = 0; i < B_bottom.size(); i ++){
    dec_mat.push_back(B_bottom[i]);
  }

  for (int i = 0; i < _w * (_n - 1) * _w / 2; i++){
    parity_dec[i] = 0;
  }
  for (int i = 0; i < _w; i++){
    for (int j = 0; j < dec_mat[i].size(); j ++){
      parity_dec[i * (_n - 1) * _w / 2 + dec_mat[i][j]] = 1;
    }
  }

}

ECDAG* BUTTERFLY::Decode(vector<int> from, vector<int> to) {
  ECDAG* ecdag = new ECDAG();
  int rBlkIdx = to[0] / _chunk_num_per_node;

  // situation (1) and (3)
  if (rBlkIdx < _k - 1 || rBlkIdx == _k) {
    vector<int> data;
    vector<int> off;
    //int matrix[160];
    int *matrix;
    matrix = new int[_w * (_n - 1) * _w / 2];

    // situation (1): data column d0 - dk-2
    if (rBlkIdx  < _k - 1 ) {
      cout << "situation (1)" << endl;
      // calculate repair sub-stripe
      for (int i = 0; i < _w; i ++){
        int row_select = floor((float)(i) / (float)(pow(2, (_k - rBlkIdx - 2))));
        if ((row_select % 4 == 0) || (row_select % 4 == 3)){
          off.push_back(i);
        }
      }
      // Debug
      // for (int i = 0; i < off.size(); i ++){
      //   cout << off[i] << " ";
      // }
      // cout << endl;

      generate_butterfly_parity_decoding_matrix_1(matrix, rBlkIdx, off);

      // Debug
      // for (int i = 0; i < _w * (_n - 1) * _w / 2; i++){
      //   cout << matrix[i] << ",";
      //   if (i % ((_n - 1) * _w / 2) == ((_n - 1) * _w / 2 - 1)){
      //     cout << endl;
      //   }
      // }
    
    }

    // situation (3): parity column 0
    if (rBlkIdx == _k) {
      cout << "situation (3)" << endl;
      for (int i = _w / 2; i < _w; i ++){
        off.push_back(i);
      }

      generate_butterfly_parity_decoding_matrix_3(matrix, rBlkIdx, off);
    }

    for (int i = 0; i < _n; i ++) {
      if (i == rBlkIdx) continue;
      for (int j = 0; j < off.size(); j ++) {
        int cid = i * _chunk_num_per_node + off[j];
        data.push_back(cid);
      }
    }
    for (int i = 0; i < to.size(); i++) {
      vector<int> coef;
      for (int j = 0; j< (_n - 1) * _w / 2; j++) {
        int c = matrix[i * (_n - 1) * _w / 2 + j];
        coef.push_back(c);
      }
      //ecdag->Join(to[i], data, coef);
 
      vector<int> new_data;
      vector<int> new_coef;
      for (int ii = 0; ii < coef.size(); ii++) {
        if (coef[ii] == 0)
          continue;
        new_data.push_back(data[ii]);
        new_coef.push_back(coef[ii]);
      }
      ecdag->Join(to[i], new_data, new_coef);
    }  
//    ecdag->BindX(to);

  // situation (2): data column dk-1
  } else if (rBlkIdx == _k - 1) {
    cout << "situation (2)" << endl;
    vector<int> off1; // for nodeid == _n - 1;
    vector<int> off2; // for other nodes
    int *matrix;
    matrix = new int[_w * (_n - 1) * _w / 2];

    for (int i = 0; i < _w; i ++){
      if (i % 2 == 1){
        off1.push_back(i);
      } else {
        off2.push_back(i);
      }
    }

    vector<int> data;
    for (int i = 0; i < _n; i++) {
      if (i == rBlkIdx) continue;
      else if (i == _n - 1) {
        for (int j = 0; j < off1.size(); j++) {
          int cid = i * _chunk_num_per_node + off1[j];
          data.push_back(cid);
        }
      } else {
        for (int j = 0; j < off2.size(); j++) {
          int cid = i * _chunk_num_per_node + off2[j];
          data.push_back(cid);
        }
      }
    }

    generate_butterfly_parity_decoding_matrix_2(matrix, rBlkIdx, off1, off2);

    for (int i = 0; i < to.size(); i++) {
      vector<int> coef;
      for (int j = 0; j < (_n - 1) * _w / 2; j ++) {
        int c = matrix[i * (_n - 1) * _w / 2 + j];
        coef.push_back(c);
      }
      //ecdag->Join(to[i], data, coef);

      vector<int> new_data;
      vector<int> new_coef;
      for (int ii = 0; ii < coef.size(); ii++) {
        if (coef[ii] == 0)
          continue;
        new_data.push_back(data[ii]);
        new_coef.push_back(coef[ii]);
      }
      ecdag->Join(to[i], new_data, new_coef);
    }
//    ecdag->BindX(to);

  // situation (4): parity column B
  } else if (rBlkIdx == _k + 1) {
    cout << "situation (4)" << endl;
    vector<int> data;
    vector<int> off0; // for nodeid=0
    for (int i = 0; i < _w / 2; i ++){
      off0.push_back(i);
    }
    for (int i = 0; i < off0.size(); i++) data.push_back(off0[i]);

    // for nodeid=1 2 3
    for (int sid = 1; sid <= _k - 1; sid++) {
      int *a;
      a = new int[_w / 2 * _w];

      vector<int> curdata;
      for (int i=0; i<_chunk_num_per_node; i++) {
        int cid = sid * _chunk_num_per_node + i;
        curdata.push_back(cid);
      }

      generate_xor_data_matrix(a, sid);

      vector<int> curres;
      for (int i=0; i < _w / 2; i++) {
        vector<int> curcoef;
        for (int j=0; j < _w; j++) curcoef.push_back(a[i * _w + j]);
        //ecdag->Join(_tmp, curdata, curcoef);

        vector<int> new_data;
        vector<int> new_coef;
        for (int ii=0; ii<curcoef.size(); ii++) {
          if (curcoef[ii] == 0)
            continue;
          new_data.push_back(curdata[ii]);
          new_coef.push_back(curcoef[ii]);
        }
        ecdag->Join(_tmp, new_data, new_coef);

        curres.push_back(_tmp);
        data.push_back(_tmp);
        _tmp++;
      }
//      ecdag->BindX(curres);
    }

    int *matrix;
    matrix = new int[_w * (_n - 1) * _w / 2];

    vector<int> off4; // for nodeid=4
    for (int i = _w / 2; i < _w; i ++){
      off4.push_back(i);
    }
    for (int i = 0; i < off4.size(); i++) data.push_back(_k * _chunk_num_per_node + off4[i]);
    
    // for (int i = 0; i < data.size(); i++) cout << data[i] << " ";
    // cout << endl;

    generate_butterfly_parity_decoding_matrix_4(matrix);

    for (int i = 0; i < to.size(); i ++) {
      vector<int> coef;
      for (int j = 0; j < (_n - 1) * _w / 2; j++) coef.push_back(matrix[i * (_n - 1) * _w / 2 + j]);
      //ecdag->Join(to[i], data, coef);

      vector<int> new_data;
      vector<int> new_coef;
      for (int ii = 0; ii < coef.size(); ii ++) {
        if (coef[ii] == 0)
          continue;
        new_data.push_back(data[ii]);
        new_coef.push_back(coef[ii]);
      }
      ecdag->Join(to[i], new_data, new_coef);
    }
//    ecdag->BindX(to);
  }
  return ecdag;
}


void BUTTERFLY::generate_encoding_matrix() {
  memset(_enc_matrix, 0, _w * _n * _w * _k *sizeof(int));

  for (int i=0; i<_sys_chunk_num; i++) {
    _enc_matrix[i*_sys_chunk_num+i]=1;
  }

  int temp=0;
  for (int i=_sys_chunk_num; i<_sys_chunk_num + _w; i++) {
    for (int j=0; j<_k; j++) {
      if (i % _w == temp) {
        _enc_matrix[i*_sys_chunk_num + _w * j + temp] = 1;
      }
    }
    temp++;
  }

  int *butterfly_parity_enc = new int[_w * _w * _k];
  generate_butterfly_parity_encoding_matrix(butterfly_parity_enc);

  // Debug
  // for (int i = 0; i < _w * _w * _k; i++){
  //   cout << butterfly_parity_enc[i] << ",";
  //   if (i % (_w * _k) == (_w * _k - 1)){
  //     cout << endl;
  //   }
  // }
  memcpy(_enc_matrix + (_k + 1) * _w * _w * _k, butterfly_parity_enc, _w * _w * _k * sizeof(int));
}


void BUTTERFLY::generate_butterfly_parity_encoding_matrix(int *parity_enc) {
  // step 1: init a data matrix(w * k)
  vector<vector<int>> init_mat;
  for (int i = 0; i < _w; i++){
    vector<int> row_list;
    for (int j = 0; j < _k; j++){
      row_list.push_back(j * _w + i);
    }
    init_mat.push_back(row_list);
  }

  // step 2: butterfly parities function
  vector<vector<int>> enc_vec = butterfly_func(init_mat, _w, _k);

  // step 3: generate butterfly parity encoding matrix
  for (int i = 0; i < _w * _w * _k; i++){
    parity_enc[i] = 0;
  }
  for (int i = 0; i < _w; i++){
    for (int j = 0; j < enc_vec[i].size(); j ++){
      parity_enc[i * _w * _k + enc_vec[i][j]] = 1;
    }
  }
}

vector<vector<int>> BUTTERFLY::perm_mat(vector<vector<int>> mat)
{
  vector<vector<int>> ret;
  for (int i = mat.size() - 1; i >= 0; i --)
  {
    ret.push_back(mat[i]);
  }
  return ret;
}

vector<vector<int>> BUTTERFLY::add_mat(vector<vector<int>> mat1, vector<vector<int>> mat2)
{
  vector<vector<int>> ret;
  for (int i = 0; i < mat1.size(); i ++)
  {
    vector<int> temp;
    for (int j = 0; j < mat1[i].size(); j ++){
      temp.push_back(mat1[i][j]);
    }
    for (int j = 0; j < mat2[i].size(); j ++){
      temp.push_back(mat2[i][j]);
    }
    ret.push_back(temp);
  }
  return ret;
}

vector<vector<int>> BUTTERFLY::butterfly_func(vector<vector<int>> mat, int m_row, int m_col)
{
  vector<vector<int>> ret;
  if ((m_row == 1) && (m_col == 1)){
    ret.push_back({mat[0][0]});
    return ret;
  }
  if ((m_row == 2) && (m_col == 2)){
    vector<int> top, lower;
    top.push_back(mat[1][0]);
    top.push_back(mat[0][1]);
    lower.push_back(mat[0][0]);
    lower.push_back(mat[0][1]);
    lower.push_back(mat[1][1]);

    ret.push_back(top);
    ret.push_back(lower);
    
    return ret;
  } else {
    // get a, b, A, B
    int half_row = m_row / 2;
    vector<vector<int>> a, b, A_mat, B_mat;

    for (int i = 0; i < half_row; i ++){
      vector<int> temp;
      temp.push_back(mat[i][0]);
      a.push_back(temp);
    }
    for (int i = half_row; i < m_row; i ++){
      vector<int> temp;
      temp.push_back(mat[i][0]);
      b.push_back(temp);
    }
    for (int i = 0; i < half_row; i ++){
      vector<int> temp;
      for (int j = 1; j < m_col; j ++){
        temp.push_back(mat[i][j]);
      }
      A_mat.push_back(temp);
    }
    for (int i = half_row; i < m_row; i ++){
      vector<int> temp;
      for (int j = 1; j < m_col; j ++){
        temp.push_back(mat[i][j]);
      }
      B_mat.push_back(temp);
    }

    // calculate top and lower
    vector<vector<int>> top, lower;
    top = add_mat(perm_mat(b), butterfly_func(A_mat, half_row, m_col - 1));
    lower = perm_mat(add_mat(add_mat(a, A_mat), butterfly_func(perm_mat(B_mat), half_row, m_col - 1)));
    
    // use top and lower to get final matrix
    for (int i = 0; i < top.size(); i ++){
      ret.push_back(top[i]);
    }
    for (int i = 0; i < lower.size(); i ++){
      ret.push_back(lower[i]);
    }
    return ret;
  }
}

//void BUTTERFLY64::Place(vector<vector<int>>& group) {
//}

