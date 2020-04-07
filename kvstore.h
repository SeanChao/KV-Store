#pragma once

#include <memory>
#include <vector>

#include "common.h"
#include "kvstore_api.h"
#include "skiplist.h"

class KVStore : public KVStoreAPI {
   public:
    KVStore(const std::string &dir);

    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

   private:
    std::string dir;
    static const uint64_t MEM_TABLE_SIZE_MAX = 2 * 1024 * 1024;

    // Maintains the size of the ss-table to generate, an data entry is composed
    // of key (8 bytes), length of string (8 bytes) and the string (length
    // byte(s)), its index infomation is key (8 bytes) and index (8 bytes). As a
    // result, the size of an entry and its offest information in the SS-Table
    // is roughly 24 + value.length()
    static const size_t DATA_CONST_SIZE = 32;
    size_t getDataSize(size_t strSize) const {
        return DATA_CONST_SIZE + strSize;
    };
    uint64_t memTableSize;

    bool verbose = true;
    std::unique_ptr<SkipList<uint64_t, std::string>> memTable;
    // a vector holds all index tables
    std::vector<std::vector<Index>> indexTableList;

    void buildSsTable();
    // reads all available SS-Table on disk and loads index tables into memory
    void readSsTable();
    // reads string in ss-table by offset, caller should ensure the key exists
    std::string readPair(uint64_t offset);
};
