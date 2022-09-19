#include "Config.hh"

Config::Config(std::string& filepath) {
    XMLDocument doc;
    doc.LoadFile(filepath.c_str());
    XMLElement* element;
    for(element = doc.FirstChildElement("setting")->FirstChildElement("attribute");
            element!=NULL;
            element=element->NextSiblingElement("attribute")){
        XMLElement* ele = element->FirstChildElement("name");
        std::string attName = ele -> GetText();
        if (attName == "controller.addr") {
            _coorIp = inet_addr(ele -> NextSiblingElement("value") -> GetText());
        } else if (attName == "agents.addr") {
            for (ele = ele -> NextSiblingElement("value"); ele != NULL; ele = ele -> NextSiblingElement("value")) {
                std::string networkloc = ele -> GetText();
                std::string tmpstring = networkloc.substr(1);
                size_t pos = tmpstring.find("/");
                std::string rack = tmpstring.substr(0, pos);
                std::string ipstr = tmpstring.substr(pos+1);
                unsigned int ip = inet_addr(ipstr.c_str());
                _agentsIPs.push_back(ip);
                _ip2Rack.insert(make_pair(ip, rack));
                std::unordered_map<std::string, std::vector<unsigned int>>::iterator it = _rack2Ips.find(rack);
                if (it != _rack2Ips.end()) {
                    _rack2Ips[rack].push_back(ip);
                } else {
                    std::vector<unsigned int> curRack;
                    curRack.push_back(ip);
                    _rack2Ips.insert(make_pair(rack, curRack));
                }
            }
        } else if (attName == "fullnode.addr"){
            for (ele = ele -> NextSiblingElement("value"); ele != NULL; ele = ele -> NextSiblingElement("value")) {
                std::string networkloc = ele -> GetText();
                std::string tmpstring = networkloc.substr(1);
                size_t pos = tmpstring.find("/");
                std::string rack = tmpstring.substr(0, pos);
                std::string ipstr = tmpstring.substr(pos+1);
                unsigned int ip = inet_addr(ipstr.c_str());
                _clientIPs.push_back(ip);
                _ip2Rack.insert(make_pair(ip, rack));
                std::unordered_map<std::string, std::vector<unsigned int>>::iterator it = _rack2Ips.find(rack);
                if (it != _rack2Ips.end()) {
                    _rack2Ips[rack].push_back(ip);
                } else {
                    std::vector<unsigned int> curRack;
                    curRack.push_back(ip);
                    _rack2Ips.insert(make_pair(rack, curRack));
                }
            }
        } else if (attName == "controller.thread.num") {
            _coorThreadNum = std::stoi(ele -> NextSiblingElement("value") -> GetText());
            
        } else if (attName == "agent.thread.num") {
            _agWorkerThreadNum = std::stoi(ele -> NextSiblingElement("value") -> GetText());
        } else if (attName == "cmddist.thread.num") {
            _distThreadNum = std::stoi(ele -> NextSiblingElement("value") -> GetText());
        } else if (attName == "local.addr") {
            _localIp = inet_addr(ele -> NextSiblingElement("value") -> GetText());
        // } else if (attName == "block.size") {
        //   _blkSize = std::stoll(ele -> NextSiblingElement("value") -> GetText());
        // } else if (attName == "packet.size") {
        //   _pktSize = std::stoi(ele -> NextSiblingElement("value") -> GetText());
        } else if (attName == "block.directory") {
            _blkDir = std::string(ele->NextSiblingElement("value")->GetText());
            //std::cout << "blkdir: " << _blkDir << std::endl;
        } else if (attName == "stripestore.directory") {
            _ssDir = std::string(ele->NextSiblingElement("value")->GetText());
        } else if (attName == "tradeoffpoint.directory") {
            _tpDir = std::string(ele->NextSiblingElement("value")->GetText());
        } else if (attName == "eccluster.size") {
            _clusterSize = std::stoi(ele -> NextSiblingElement("value") -> GetText());  
        } else if (attName == "repair.thread.num") {
            _rpThreads = std::stoi(ele -> NextSiblingElement("value") -> GetText());
        } else if (attName == "network.bandwidth") {
            _netbdwt = std::stoi(ele -> NextSiblingElement("value") -> GetText());
        }
    }

//   _fsFactory.insert(make_pair(_fsType, _fsParam));
}

Config::~Config() {
//  for (auto it = _ecPolicyMap.begin(); it != _ecPolicyMap.end(); it++) {
//    ECPolicy* ecpolicy = it->second;
//    delete ecpolicy;
//  }
}

