#include "StripeMeta.hh"

StripeMeta::StripeMeta(std::string& stripename, std::string& filepath) {
  //_stripeName = stripename;

  XMLDocument doc;
  doc.LoadFile(filepath.c_str());
  XMLElement* element;
  for(element = doc.FirstChildElement("stripe")->FirstChildElement("attribute");
      element!=NULL;
      element=element->NextSiblingElement("attribute")){
    XMLElement* ele = element->FirstChildElement("name");
    std::string attName = ele -> GetText();
    if (attName == "code") {
      _codeName = std::string(ele -> NextSiblingElement("value") -> GetText());
      //std::cout << "codename: " << _codeName << std::endl;
    } else if (attName == "ecn") {
      _ecN = std::stoi(ele -> NextSiblingElement("value") -> GetText());
      //std::cout << "ecn: " << _ecN << std::endl;
    } else if (attName == "eck") {
      _ecK = std::stoi(ele -> NextSiblingElement("value") -> GetText());
      //std::cout << "eck: " << _ecK << std::endl;
    } else if (attName == "ecw") {
      _ecW = std::stoi(ele -> NextSiblingElement("value") -> GetText());
      //std::cout << "ecw: " << _ecW << std::endl;
    } else if (attName == "stripename") {
      _stripeName = std::string(ele -> NextSiblingElement("value") -> GetText());
    } else if (attName == "blocklist") {
      for (ele = ele -> NextSiblingElement("value"); ele != NULL; ele = ele -> NextSiblingElement("value")) {
        std::string tmps = ele -> GetText();
        vector<std::string> splits = DistUtil::splitStr(tmps, ":");
        _blockList.push_back(splits[0]);
        _locList.push_back(inet_addr(splits[1].c_str()));
        //std::cout << splits[0] << ": " << splits[1] << std::endl; 
      }
    } else if (attName == "blockbytes") {
        _blkbytes = std::stoll(ele -> NextSiblingElement("value") -> GetText());
    } else if (attName == "pktbytes") {
        _pktbytes = std::stoi(ele -> NextSiblingElement("value") -> GetText());
    }
  }

  // revise pktbytes when ecw > 1
  _pktbytes = _pktbytes * _ecW;
}

std::string StripeMeta::getStripeName() {
  return _stripeName;
}

int StripeMeta::getECN() {
  return _ecN;
}

int StripeMeta::getECK() {
  return _ecK;
}

int StripeMeta::getECW() {
  return _ecW;
}

vector<std::string> StripeMeta::getBlockList() {
  return _blockList;
}

vector<unsigned int> StripeMeta::getLocList() {
  return _locList;
}

int StripeMeta::getBlockIndex(string blockname) {
  int toret = -1;
  for (int i=0; i<_blockList.size(); i++) {
    if (blockname == _blockList[i]) {
      toret = i;
      break;
    }
  }
  return toret;
}

ECBase* StripeMeta::createECClass() {
  ECBase* toret = NULL;
  if (_codeName == "Clay") {
      cout << "StripeMeta::createECClass Clay" << endl;
      toret = new Clay(_ecN, _ecK, _ecW, {to_string(_ecN-1)});
  } else if (_codeName == "RSCONV") {
      cout << "StripeMeta::createECClass RSCONV" << endl;
      toret = new RSCONV(_ecN, _ecK, _ecW, {});
  } else if (_codeName == "RSPIPE") {
      cout << "StripeMeta::createECClass RSPIPE" << endl;
      toret = new RSPIPE(_ecN, _ecK, _ecW, {});
  } else if (_codeName == "BUTTERFLY") {
      cout << "StripeMeta::createECClass BUTTERFLY" << endl;
      toret = new BUTTERFLY(_ecN, _ecK, _ecW, {});
  }
  return toret;
}

std::string StripeMeta::getCodeName() {
    return _codeName;
}

long long StripeMeta::getBlockBytes() {
    return _blkbytes;
}

int StripeMeta::getPacketBytes() {
    return _pktbytes;
}

void StripeMeta::updateLocForBlock(string block, vector<unsigned int> all_ips) {
    // 0. figure out the block index
    int index;
    unsigned int ip_old;
    for (int i=0; i<_ecN; i++) {
        if (_blockList[i] == block) {
            index = i;
            ip_old = _locList[i];
            break;
        }
    }
    
    // 1. get candidates
    vector<unsigned int> candidates;
    for (int i=0; i<all_ips.size(); i++) {
        unsigned int ip = all_ips[i];
        if (find(_locList.begin(), _locList.end(), ip) == _locList.end())
            candidates.push_back(ip);
    }

    assert(candidates.size() > 0);
    // 2. randomly choose an ip from candidates
    srand(time(0));
    int idx = rand()%candidates.size();
    unsigned int ip_new = candidates[idx];

    // 3. update the new ip
    _locList[index] = ip_new;
}

