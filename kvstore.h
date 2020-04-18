#pragma once

#include <memory>
#include <tuple>
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

    void trigger();  // debug TODO: delete

   private:
    std::string dir;
    bool verbose = true;

    static const uint64_t MEM_TABLE_SIZE_MAX = 2 * 1024 * 1024;
    // static const uint64_t MEM_TABLE_SIZE_MAX = 200;

    // Maintains the size of the ss-table to generate, an data entry is composed
    // of key (8 bytes), length of string (8 bytes), a timestamp (8 bytes) and
    // the string (length byte(s)), its index infomation is key (8 bytes) and
    // index (8 bytes). As a result, the size of an entry and its offest
    // information in the SS-Table is roughly 40 + value.length()
    static const size_t DATA_CONST_SIZE = 40;

    size_t getDataSize(size_t strSize) const {
        return DATA_CONST_SIZE + strSize;
    };

    int levelSizeLim(int level) const { return (1 << (level + 1)); }

    uint64_t memTableSize;

    std::unique_ptr<SkipList<uint64_t, std::string>> memTable;

    // a vector holds all index tables
    std::vector<std::vector<Index>> indexTableList;

    int level;                 // the number of current levels
    std::vector<int> fileNum;  // the number of ss-tables in each level

    // turns memTable into ssTable and resets memTable
    void convertMemTable();

    // loads all available SS-Table on disk into memory
    void loadSsTable();

    // loads index tables into memory, the function should only be called on
    // startup in sequence
    void readSsTable(std::string path);

    std::vector<Pair> readSsTable(int level, int id);

    void writeEntry(std::fstream &fs, Entry &e);

    // Finds the given key in index tables and return the index of the table in
    // index table list. Return value -1 indicates the key doesn't exist.
    // Saves the offset of the entry in optional parameter offsetDst.
    int findIndexedKey(uint64_t key, uint64_t *offsetDst = nullptr) const;

    // resolves the path of sstable x in level y
    std::string resolvePath(int level, int id) const;

    // resolves the path of level, if -1 is passed, returns temporary folder
    std::string resolvePath(int level) const;

    std::string resolvePath(Location &l) const;

    // reads string in ss-table by offset, caller should ensure the key exists
    std::string readPair(std::string path, uint64_t offset);

    // resets memTable and related data
    void resetMemTable();

    // Writes ssTable to specified location.
    // If tmp is set to true, then the function would writes to a temporary
    // folder, with file named with l.id
    // -> write to tmp and mv them later
    void writeSsTable(std::vector<Pair> table, Location l, bool tmp = false);

    // void compaction();

    // performs compaction on specified level other than level 0
    void compaction(int level = 0);

    // merges a and b
    std::vector<Pair> merge(std::vector<Pair> a, std::vector<Pair> b);

    void renameLevel(int level, int mode);

    // returns the index of target cached indexTable in indexTableList
    int getIndex(int level, int id) const;
    int getIndex(Location &loc) const;

    // returns the range of keys in file with id in level
    std::tuple<uint64_t, uint64_t> getKeyRange(int level, int id);
};
