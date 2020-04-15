#pragma once
#include <string>

struct Pair {
    uint64_t key;
    std::string val;
    Pair(uint64_t key, std::string val) : key(key), val(val) {}
};

struct Index {
    uint64_t key;
    uint64_t offset;
    Index(uint64_t key, uint64_t offset) : key(key), offset(offset) {}
};

struct Location {
    int level;
    int id;
    Location(int level, int id) : level(level), id(id) {}
};
