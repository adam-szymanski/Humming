#pragma once

#include <string>
#include <vector>
#include <iostream>

using namespace std;

namespace humming::DB {

extern hash<string> hasher;

struct KV {
  string _k;
  string _v;
  size_t _hash;
  KV() {}
  KV(string &&k, string &&v) : _k(k), _v(v), _hash(hasher(_k)) {}
  KV(KV &&other) = default;
  KV &operator=(KV &&) = default;
};

typedef vector<KV> KVs;

}

template<typename T>
T &operator<<(T &os, const humming::DB::KV &kv) {
  os << "{" << kv._k << ": " << kv._v << "}";
  return os;
}

template<typename T>
T&operator<<(T &os, const humming::DB::KVs &kvs) {
  os << "[";
  for (size_t i = 0; i < kvs.size(); ++i) {
    if (i > 0)
      os << ", ";
    os << kvs[i];
  }
  os << "]";
  return os;
}
