#include "TradeoffPoints.hh"

TradeoffPoints::TradeoffPoints(std::string filepath) {
    XMLDocument doc;
    doc.LoadFile(filepath.c_str());
    XMLElement* element;
    for(element = doc.FirstChildElement("tradeoff")->FirstChildElement("attribute");
            element!=NULL;
            element=element->NextSiblingElement("attribute")) {
        XMLElement* ele = element->FirstChildElement("name");
        std::string attName = ele -> GetText();
        if (attName == "code") {
            _codeName = std::string(ele -> NextSiblingElement("value") -> GetText());
        } else if (attName == "ecn") {
            _ecN = std::stoi(ele -> NextSiblingElement("value") -> GetText());
        } else if (attName == "eck") {
            _ecK = std::stoi(ele -> NextSiblingElement("value") -> GetText());
        } else if (attName == "ecw") {
            _ecW = std::stoi(ele -> NextSiblingElement("value") -> GetText());
        } else if (attName == "digits") {
            _digits = std::stoi(ele -> NextSiblingElement("value") -> GetText());
        } else if (attName == "point") {
            for (ele = ele -> NextSiblingElement("value"); ele != NULL; ele = ele -> NextSiblingElement("value")) {
                std::string tmps = ele -> GetText();
                vector<std::string> splits = DistUtil::splitStr(tmps, ":");
                int idx = std::stoi(splits[0]);
                std::string coloring = splits[1];
                _points.insert(make_pair(idx, coloring));
            }
        }
    }
}

vector<int> TradeoffPoints::getColoringByIdx(int idx) {
    vector<int> toret;
    assert(idx < _ecN);

    string tmps = _points[idx];
    for (int i=0; i<tmps.length(); i+=_digits) {
        int v = std::stoi(tmps.substr(i, _digits));
        toret.push_back(v);
    }
    return toret;
}
