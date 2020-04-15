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
    : KVStoreAPI(dir), dir(dir), verbose(false), memTableSize(0), level(0) {
    memTable = std::unique_ptr<SkipList<uint64_t, std::string>>(
        new SkipList<uint64_t, std::string>());
    // this->dir = dir;
    // root = std::filesystem::path(dir);
    fileNum.push_back(0);
    loadSsTable();
}

KVStore::~KVStore() {
    std::cout << "call destructor" << std::endl;
    memTable.release();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
    memTable->put(key, s);
    memTableSize += getDataSize(s.length());
    if (verbose) {
        std::cerr << "put " << key << '\t' << s << "  done.\n";
        std::cerr << *memTable << '\n';
    }
    // if the size memTable reaches the threshold, then calls buildSsTable and
    // resets memTable
    if (this->memTableSize >= MEM_TABLE_SIZE_MAX) {
        convertMemTable();
        resetMemTable();
        // std::cout << *memTable << std::endl;
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
    // looks for key in SsTables using indexTable
    uint64_t offset = 0;
    // uint64_t len = 0;
    bool found = false;
    int count = 0;
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
        if (!found) count++;
    }
    int level = 0;
    while (
        !(count >= (1 << (level + 1)) - 2 && count <= (1 << (level + 2)) - 3)) {
        level++;
    }
    int fileId = count - ((1 << (level + 1)) - 2);
    std::cout << "level: " << level << " num: " << fileId << std::endl;
    if (found) {
        // reads value on disk according to offest
        return readPair(resolvePath(level, fileId), offset);
    }
    return "";
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 *
 * TODO: support ss table
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

void KVStore::convertMemTable() {
    std::cout << "call convertMemTable, writing to " << this->dir << std::endl;
    // if (memTableSize < MEM_TABLE_SIZE_MAX) return;
    // get pointer to the head of linked list from SkipList. Beware that the
    // last non-nullptr pointer would be the tail, which contains no meaningful
    // data
    std::shared_ptr<typename SkipList<uint64_t, std::string>::Node> p =
        memTable->exportData();
    // while ((p = p->succ) && memTable->valid(p)) std::cout << p->key << "\n";
    // TODO: IO exception
    std::filesystem::path root(this->dir);
    std::filesystem::path lv("level-0");
    std::string filename = "sstable-" + std::to_string(fileNum[level]);
    std::filesystem::create_directories(root / lv);
    std::fstream fs((root / lv / filename), std::ios::out | std::ios::binary);
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
    // update state
    fileNum[0]++;
    // compaction if the number of files in level 0 is more than 2
    if (fileNum[0] > 2) compaction();
}

void KVStore::loadSsTable() {
    // checks existing ss-table
    std::filesystem::path root(this->dir);
    int lv = 0;
    int count = 0;
    std::filesystem::path level(root / ("level-" + std::to_string(lv)));
    while (std::filesystem::exists(level)) {
        std::string filename = "sstable-" + std::to_string(count);
        std::cerr << "see lv " << lv << std::endl;
        while (std::filesystem::exists(level / filename)) {
            std::cerr << "see file " << filename << std::endl;
            // parse the file
            std::string filepath = (level / filename).string();
            readSsTable(filepath);
            filename = "sstable-" + std::to_string(++count);
        }
        count = 0;
        level = root / ("level-" + std::to_string(++lv));
    }
}

void KVStore::readSsTable(std::string path) {
    // load index table into indexTableList
    // std::filesystem::path ssDir(this->dir);
    // std::string file("sstable-test.dat");
    std::ifstream fs(path, std::ios::binary);
    std::cout << "read ss table from " << path << std::endl;
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
    indexTableList.push_back(indexTable);  // FIXME: insert to level 0
}

std::vector<Pair> KVStore::readSsTable(int level, int id) {
    std::string path = resolvePath(level, id);
    std::ifstream fs(path, std::ios::binary);
    std::cout << "read ss table from " << path << std::endl;
    fs.seekg(-8, std::ios::end);
    uint64_t offset = 0;
    fs.read(reinterpret_cast<char *>(&offset), sizeof(offset));
    // std::cout << "read: offset of index table at " << std::hex << offset
    //           << std::dec << std::endl;

    std::vector<Pair> table;
    fs.seekg(std::ios::beg);
    while (fs.tellg() < (int64_t)offset) {
        std::cout << "fs pos: " << fs.tellg() << std::endl;
        uint64_t key = 0;
        fs.read(reinterpret_cast<char *>(&key), sizeof(key));
        uint64_t len = 0;
        fs.read(reinterpret_cast<char *>(&len), sizeof(len));
        char *tmp = new char[len + 1];
        fs.read(tmp, len);
        tmp[len] = '\0';
        std::string val(tmp);
        delete[] tmp;
        table.push_back(Pair(key, val));
    }
    for (auto i : table)
        std::cout << "key " << i.key << "\tval " << i.val << std::endl;

    return table;
}

std::string KVStore::resolvePath(int level, int id) const {
    std::filesystem::path root(this->dir);
    std::string lv = "level-" + std::to_string(level);
    std::string filename = "sstable-" + std::to_string(id);
    return std::filesystem::path(root / lv / filename).string();
}

std::string KVStore::resolvePath(int level) const {
    std::filesystem::path root(this->dir);
    std::string lv = "level-" + std::to_string(level);
    return std::filesystem::path(root / lv).string();
}

std::string KVStore::resolvePath(Location &l) const {
    return resolvePath(l.level, l.id);
}

std::string KVStore::readPair(std::string path, uint64_t offset) {
    // std::filesystem::path ssDir(this->dir);
    // std::string file("sstable-test.dat");
    std::ifstream fs(path, std::ios::binary);
    uint64_t len = 0;
    fs.seekg(offset + 8);
    fs.read(reinterpret_cast<char *>(&len), sizeof(len));
    char *tmp = new char[len + 1];
    fs.read(tmp, len);
    tmp[len] = '\0';
    std::string val(tmp);
    delete[] tmp;
    std::cout << "read " << len << " byte: " << val << std::endl;
    delete[] tmp;
    return val;
}

void KVStore::resetMemTable() {
    // resets memTable
    memTable.release();
    memTable = std::unique_ptr<SkipList<uint64_t, std::string>>(
        new SkipList<uint64_t, std::string>());
    // reset state
    memTableSize = 0;
}

void KVStore::writeSsTable(std::vector<Pair> table, Location loc) {
    std::string path = resolvePath(loc.level, loc.id);
    std::string folder = resolvePath(loc.level);
    if (!std::filesystem::exists(folder))
        std::filesystem::create_directories(folder);
    std::fstream fs(path, std::ios::out | std::ios::binary);
    if (!fs.is_open()) {
        std::cout << "[writeSsTable] Failed to open sstable file " << path
                  << std::endl;
        return;
    }
    // prepares the cache for index data
    std::vector<Index> indexTable;
    uint64_t offset = 0;
    // writes the data segment
    for (auto &p : table) {
        uint64_t strLen = p.val.length();
        fs.write(reinterpret_cast<char *>(&p.key), sizeof(p.key));
        fs.write(reinterpret_cast<char *>(&strLen), sizeof(strLen));
        fs.write(p.val.c_str(), strLen);  // write val
        // caches index data
        indexTable.push_back(Index(p.key, offset));
        offset += sizeof(p.key) + sizeof(strLen) + p.val.length();
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
    fs.close();
    // update state
    indexTableList.insert(indexTableList.begin() + getIndex(loc), indexTable);
    if ((int)fileNum.size() <= loc.level) fileNum.push_back(0);
    fileNum[loc.level]++;

    std::cout << "build ss table done." << std::endl;
}

void KVStore::compaction(int level) {
    std::cout << "run compaction on level " << level << std::endl;
    // range statistics
    int nextLv = level + 1;
    int nextLvPos = -1;        // index of insertion point of indexTable in next
                               // level, -1 indicates no assignment yet
    std::vector<Location> id;  // location of tables to be merged
    uint64_t min = UINT64_MAX;
    uint64_t max = 0;
    if (level == 0) {
        uint64_t t0Min = indexTableList[0].front().key;
        uint64_t t1Min = indexTableList[1].front().key;
        uint64_t t0Max = indexTableList[0].back().key;
        uint64_t t1Max = indexTableList[1].back().key;
        uint64_t t2Min = indexTableList[2].front().key;
        uint64_t t2Max = indexTableList[2].back().key;
        min = std::min({t0Min, t1Min, t2Min});
        max = std::max({t0Max, t1Max, t2Max});
        std::cerr << "[lv0]compaction range " << min << " " << max << std::endl;
        id.push_back(Location(0, 0));
        id.push_back(Location(0, 1));
        id.push_back(Location(0, 2));
    } else {
        // selects exceeding files and merges them into next level
        int more = fileNum[level] - levelSizeLim(level);
        // counts the range of keys in the last file of current level
        for (int i = 0; i < more; i++) {
            int tabId = getIndex(level, levelSizeLim(level) + i);
            uint64_t tMin = indexTableList[tabId].front().key;
            uint64_t tMax = indexTableList[tabId].back().key;
            if (tMin < min) min = tMin;
            max = tMax > max ? tMax : max;
            id.push_back(Location(level, levelSizeLim(level) + i));
        }
    }
    for (int i = 0; i < fileNum[nextLv]; i++) {
        int tabId = getIndex(nextLv, i);
        uint64_t tMin = indexTableList[tabId].front().key;
        uint64_t tMax = indexTableList[tabId].back().key;
        if (!(tMax < min || max < tMin)) {
            // range overlaps
            id.push_back(Location(nextLv, i));
            if (nextLvPos == -1) nextLvPos = i;
        }
    }
    // merge
    std::vector<Pair> all;
    for (size_t i = 0; i < id.size(); i++) {
        // reads ssTable and build pair vector
        std::vector<Pair> t = readSsTable(id[i].level, id[i].id);
        all = merge(all, t);
    }
    std::cout << "Merged all data: " << std::endl;
    for (auto &i : all)
        std::cout << "key: " << i.key << " val: " << i.val << std::endl;
    // update state: removes indexTable from memory and update fileNum
    for (int i = (int)id.size() - 1; i >= 0; i--) {
        indexTableList.erase(indexTableList.begin() + getIndex(id[i]));
        fileNum[id[i].level]--;
        if (!std::filesystem::remove(resolvePath(id[i])))
            std::cout << "error deleting " << resolvePath(id[i]) << std::endl;
    }
    // slice merged data and write to disk
    // slice all data into id.size() pieces and writes them to disk
    uint64_t size = 0;
    std::vector<Pair> tmp;
    if (nextLvPos == -1) nextLvPos = 0;
    for (auto &i : all) {
        size += i.val.length() + DATA_CONST_SIZE;
        tmp.push_back(i);
        if (size >= MEM_TABLE_SIZE_MAX) {
            writeSsTable(tmp, Location(nextLv, nextLvPos));
            size = 0;
            nextLvPos++;
            tmp.clear();
        }
    }
    // program state is updated in call to writeSsTable
    // call compaction on next level if necessary
    if (fileNum[nextLv] > levelSizeLim(nextLv)) compaction(nextLv);
}

std::vector<Pair> KVStore::merge(std::vector<Pair> a, std::vector<Pair> b) {
    std::vector<Pair> tmp;
    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        if (a[i].key < b[j].key)
            tmp.push_back(a[i++]);
        else
            tmp.push_back(b[j++]);
    }
    while (i < a.size()) {
        tmp.push_back(a[i++]);
    }
    while (j < b.size()) {
        tmp.push_back(b[j++]);
    }
    std::cout << "tmp merged: " << std::endl;
    for (auto i : tmp) {
        std::cout << i.key << " " << i.val << std::endl;
    }
    return tmp;
}

int KVStore::getIndex(int level, int id) const {
    int index = 0;
    for (size_t i = 0; i < fileNum.size() && i < (size_t)level - 1; i++) {
        index += fileNum[i];
    }
    index += id;
    return index;
}

int KVStore::getIndex(Location &loc) const {
    return getIndex(loc.level, loc.id);
}

std::tuple<uint64_t, uint64_t> KVStore::getKeyRange(int level, int id) {
    // uint64_t min = indexTable
    return {1, 2};
}

void KVStore::trigger() { convertMemTable(); }
