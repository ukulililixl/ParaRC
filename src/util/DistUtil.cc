#include "DistUtil.hh"

double DistUtil::duration(struct timeval t1, struct timeval t2) {
  return (t2.tv_sec-t1.tv_sec) * 1000.0 + (t2.tv_usec-t1.tv_usec) / 1000.0;
}

vector<string> DistUtil::str2container(string line) {
  int start = 0;
  int pos = line.find_first_of(";");
  vector<string> entryitems;
  while (pos != string::npos) {
    string item = line.substr(start, pos - start);
    start = pos + 1;
    pos = line.find_first_of(";", start);
    entryitems.push_back(item);
  }
  string item = line.substr(start, pos - start);
  entryitems.push_back(item);
  return entryitems;
}

vector<string> DistUtil::splitStr(string s, string delimiter) {
  int start = 0;
  int pos = s.find_first_of(delimiter);
  vector<string> entryitems;
  while (pos != string::npos) {
    string item = s.substr(start, pos - start);
    start = pos + 1;
    pos = s.find_first_of(delimiter, start);
    entryitems.push_back(item);
  }
  string item = s.substr(start, pos - start);
  entryitems.push_back(item);
  return entryitems;
}

vector<string> DistUtil::listFiles(string dirpath) {
  vector<string> toret;

  DIR *dir;
  struct dirent *ent;
  assert((dir = opendir (dirpath.c_str())) != NULL);
    /* print all the files and directories within directory */
  while ((ent = readdir (dir)) != NULL) {
    string s(ent->d_name);
    if (s == "." || s == "..")
      continue;
    if (s[0] == '.')
      continue;
    toret.push_back(ent->d_name);
  }
  closedir (dir);
  return toret;
}

double DistUtil::average(vector<int> list) {
  double toret = 0.0;

  for (int i=0; i<list.size(); i++)
    toret += double(list[i]);

  return toret/list.size();
}

double DistUtil::variance(vector<int> list, double mean) {
  double sum = 0.0;

  for (int i=0; i<list.size(); i++) {
    sum += pow(list[i] - mean, 2);
  }

  return sum / (list.size() - 2);
}

int DistUtil::ndigits(int m) {
    int digits = 0;
    while (m>0) {
        digits++;
        m=m/10;
    }
    return digits;
}

string DistUtil::num2str(int num, int digits) {
    string toret;
    while (num>0) {
        int r = num % 10;
        toret = to_string(r) + toret;
        num = num/10;
    }
    while (toret.length() < digits) {
        toret = "0"+toret;
    }
    return toret;
}

string DistUtil::vec2str(vector<int> v, int digits) {
    string toret;
 
    for (int i=0; i<v.size(); i++) {
        toret += DistUtil::num2str(v[i], digits);
    } 
    return toret;
}
