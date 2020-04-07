#include "kvstore.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "common.h"
#include "skiplist.h"

KVStore::KVStore(const std::string &dir)
    : KVStoreAPI(dir), memTableSize(0), verbose(false) {
    memTable = std::unique_ptr<SkipList<uint64_t, std::string>>(
        new SkipList<uint64_t, std::string>());
    this->dir = dir;
    readSsTable();
}

KVStore::~KVStore() {}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
int tmpCounter = 0;
void KVStore::put(uint64_t key, const std::string &s) {
    tmpCounter++;
    memTable->put(key, s);
    memTableSize += getDataSize(s.length());
    if (verbose) {
        std::cerr << "put " << key << '\t' << s << "  done.\n";
        std::cerr << *memTable << '\n';
    }
    // if the size memTable reaches the threshold, then calls buildSsTable
    if (this->memTableSize >= MEM_TABLE_SIZE_MAX) {
        buildSsTable();
    }
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 *
 * Looks for key in memTable first, and then in SsTables
 */
std::string KVStore::get(uint64_t key) {
    // if (verbose) std::cerr << "try get " << key;
    // looks for key in memTable
    std::string *strPointer = memTable->get(key);
    if (verbose && strPointer)
        std::cerr << " -> " << *strPointer << '\n';
    else if (verbose)
        std::cerr << " NOT FOUND in memTable.\n";
    if (strPointer) return *strPointer;
    return "";  // debug point
    // looks for key in SsTables using indexTable
    uint64_t offset = 0;
    // uint64_t len = 0;
    bool found = false;
    for (auto table : indexTableList) {
        if (found) break;
        // std::cout << "looks for key " << key << " in indexTable" <<
        // std::endl; binary searches in an index table
        int l = 0;
        int r = table.size() - 1;
        while (l <= r) {
            int mid = (l + r) / 2;
            // std::cout << "mid: " << mid << " l" << l << " r" << r <<
            // std::endl;
            if (table[mid].key == key) {
                std::cout << "found " << table[mid].key << "@"
                          << table[mid].offset << std::endl;
                offset = table[mid].offset;
                found = true;
                break;
            }
            if (table[mid].key < key) {
                l = mid + 1;
            } else
                r = mid - 1;
        }
    }
    if (found) {
        // reads value on disk according to offest
        return readPair(offset);
    }
    return "";
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    if (verbose) {
        std::cerr << "remove " << key << "\n";
        std::cerr << *memTable;
    }
    std::shared_ptr<std::string> strVal(new std::string);
    if (memTable->remove(key, strVal)) {
        this->memTableSize -= getDataSize((*strVal).length());
        return true;
    } else
        return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    // TODO: reset
}

void KVStore::buildSsTable() {
    std::cout << "call buildSsTable, writing to " << this->dir << std::endl;
    if (memTableSize < MEM_TABLE_SIZE_MAX) return;
    // get pointer to the head of linked list from SkipList. Beware that the
    // last non-nullptr pointer would be the tail, which contains no meaningful
    // data
    std::shared_ptr<typename SkipList<uint64_t, std::string>::Node> p =
        memTable->exportData();
    // while ((p = p->succ) && memTable->valid(p)) std::cout << p->key << "\n";
    // TODO: IO exception
    std::filesystem::path sDir(this->dir);
    std::string filename("sstable-test.dat");
    std::filesystem::create_directories(sDir);
    std::fstream fs((sDir / filename).string(),
                    std::ios::out | std::ios::binary);
    // prepares the cache for index data
    std::vector<Index> indexTable;
    uint64_t offset = 0;
    // writes the data segment
    while ((p = p->succ) && memTable->valid(p)) {
        // std::cout << "write key = " << p->key << std::endl;
        // convert to char * on demand
        uint64_t strLen = p->val.length();
        fs.write(reinterpret_cast<char *>(&p->key), sizeof(p->key));
        fs.write(reinterpret_cast<char *>(&strLen), sizeof(strLen));
        fs.write(p->val.c_str(), strLen);  // write val
        // caches index data
        indexTable.push_back(Index(p->key, offset));
        offset += sizeof(p->key) + sizeof(strLen) + p->val.length();
    }
    // writes index data to file
    for (auto i : indexTable) {
        std::cout << "write key = " << i.key << " offset = " << i.offset
                  << std::endl;
        fs.write(reinterpret_cast<char *>(&i.key), sizeof(i.key));
        fs.write(reinterpret_cast<char *>(&i.offset), sizeof(i.offset));
    }
    // writes meta data
    // index of indexTable
    std::cout << "Meta: offset = " << std::hex << "0x" << offset << std::dec
              << std::endl;
    fs.write(reinterpret_cast<char *>(&offset), sizeof(offset));
    indexTableList.push_back(indexTable);
    fs.close();
    std::cout << "build ss table done." << std::endl;
}

void KVStore::readSsTable() {
    // load index table into indexTableList
    std::filesystem::path ssDir(this->dir);
    std::string file("sstable-test.dat");
    std::ifstream fs((ssDir / file).string(), std::ios::binary);
    // std::cout << (ssDir / file).string() << std::endl;
    // reads the offest of the begining of the index part
    fs.seekg(-8, std::ios::end);
    uint64_t offset = 0;
    fs.read(reinterpret_cast<char *>(&offset), sizeof(offset));
    std::cout << "read: get offset of index table at " << std::hex << offset
              << std::dec << std::endl;
    // reads keys and indices and saves them in indexTable
    // auto endData = fs.seekg(-8, std::ios::end);
    fs.seekg(offset);
    std::vector<Index> indexTable;
    while (!fs.eof()) {
        uint64_t key = 0;
        uint64_t off = 0;
        fs.read(reinterpret_cast<char *>(&key), sizeof(key));
        fs.read(reinterpret_cast<char *>(&off), sizeof(off));
        if (fs.gcount() == 0)
            break;  // if fs comes to meta data then break, this condition may
                    // fail if the meta data changes
        std::cout << "read key: " << key << std::hex << " @0x" << off << " "
                  << fs.gcount() << std::endl;
        indexTable.push_back(Index(key, off));
    }
    // for(auto i: indexTable) {
    //     std::cout << "key " << i.key << "offset "
    // }
    indexTableList.push_back(indexTable);
}

std::string KVStore::readPair(uint64_t offset) {
    std::filesystem::path ssDir(this->dir);
    std::string file("sstable-test.dat");
    std::ifstream fs((ssDir / file).string(), std::ios::binary);
    uint64_t len = 0;
    fs.seekg(offset + 8);
    fs.read(reinterpret_cast<char *>(&len), sizeof(len));
    char *tmp = new char[len + 1];
    fs.read(tmp, len);
    tmp[len] = '\0';
    std::string val(tmp);
    std::cout << "read " << len << " byte: " << val << std::endl;
    delete[] tmp;
    return val;
}
