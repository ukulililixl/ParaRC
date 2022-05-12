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
    }
  }
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
  }
  return toret;
}