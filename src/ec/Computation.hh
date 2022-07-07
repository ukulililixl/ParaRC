#ifndef _COMPUTATION_HH_
#define _COMPUTATION_HH_

#include "../inc/include.hh"
#include "../util/galois.h"
#include "../util/jerasure.h"

#include <isa-l.h>

#define GF_W 8

using namespace std;

class Computation {
  public:
    static mutex _cLock;
    static int singleMulti(int a, int b, int w);
    static void Multi(char** dst, char** src, int* mat, int rowCnt, int colCnt, int len, string lib);
    static void JerasureInvertMatrix(int* mat1, int* mat2, int m, int n);
    static int* JerasureMatrixMultiply(int* mat1, int* mat2, int a1, int b1, int a2, int b2, int w);
};

#endif
