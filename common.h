#pragma once
#include <iostream>
#include <string>

struct Pair {
    uint64_t key;
    int64_t time;
    std::string val;
    Pair(uint64_t key, std::string val) : key(key), val(val) {}
    Pair(uint64_t key, int64_t time, std::string val)
        : key(key), time(time), val(val) {}
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

struct Entry {
    uint64_t key;
    time_t timestamp;
    uint64_t len;
    std::string str;
    Entry(uint64_t key, time_t timestamp, uint64_t len, std::string str)
        : key(key), timestamp(timestamp), len(len), str(str) {}
};
