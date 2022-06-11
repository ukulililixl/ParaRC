#include "StripeStore.hh"

StripeStore::StripeStore(Config* conf) {
  _conf = conf;
  string ssdir = conf->_ssDir;
  cout << "StripeStore::StripeStore ssdir: " << ssdir << endl;

  // read each file in ssdir and generate
  vector<string> stripenamelist = DistUtil::listFiles(ssdir);  
  for (string item: stripenamelist) {
    string fullpath = ssdir+"/"+item;
    cout << fullpath << endl;
    vector<string> splititem = DistUtil::splitStr(item, ".");
    StripeMeta* meta = new StripeMeta(splititem[0], fullpath); 
    string stripename = meta->getStripeName();
    _stripeMetaMap.insert(make_pair(stripename, meta));

    for (string blkname: meta->getBlockList()) {
      _blk2stripe.insert(make_pair(blkname, stripename));
    }
  }

  string tpdir = conf->_tpDir;
  cout << "StripeStore::tradeoffpoint dir: " << tpdir << endl;
  
  // read each file in ssdir and generate
  vector<string> tplist = DistUtil::listFiles(tpdir);
  for (string item: tplist) {
      string fullpath = tpdir + "/" + item;
      cout << fullpath << endl;
      vector<string> splititem = DistUtil::splitStr(item, ".");
      TradeoffPoints* point = new TradeoffPoints(fullpath);
      _tradeoffPointsMap.insert(make_pair(splititem[0], point));
  }

}

StripeMeta* StripeStore::getStripeMetaFromBlockName(string blockname) {
  StripeMeta* toret = NULL;

  if (_blk2stripe.find(blockname) !=  _blk2stripe.end()) {
    string stripename = _blk2stripe[blockname];
    toret = _stripeMetaMap[stripename];
  }

  return toret;
}

StripeMeta* StripeStore::getStripeMetaFromStripeName(string stripename) {
  StripeMeta* toret = NULL;
  if (_stripeMetaMap.find(stripename) != _stripeMetaMap.end()) {
    toret = _stripeMetaMap[stripename];
  }
  return toret;
}

TradeoffPoints* StripeStore::getTradeoffPoints(string tpentry) {
    TradeoffPoints* toret = NULL;

    if (_tradeoffPointsMap.find(tpentry) != _tradeoffPointsMap.end()) {
        toret = _tradeoffPointsMap[tpentry];
    }

    return toret;
}

