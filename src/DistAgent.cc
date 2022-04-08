#include "common/Config.hh"
#include "common/Worker.hh"

#include "inc/include.hh"

using namespace std;

int main(int argc, char** argv) {

  string configPath = "conf/sysSetting.xml";
  Config* conf = new Config(configPath);

  Worker** workers = (Worker**)calloc(conf -> _agWorkerThreadNum, sizeof(Worker*)); 

  thread thrds[conf -> _agWorkerThreadNum];
  for (int i = 0; i < conf -> _agWorkerThreadNum; i ++) {
    workers[i] = new Worker(conf, i);
    thrds[i] = thread([=]{workers[i] -> doProcess();});
  }
  cout << "OECAgent started ..." << endl;

  /**
   * Shoule never reach here
   */
  for (int i = 0; i < conf -> _agWorkerThreadNum; i ++) {
    thrds[i].join();
  }
  for (int i=0; i<conf -> _agWorkerThreadNum; i++) {
    delete workers[i];
  }
  free(workers);
  delete conf;

  return 0;
}

