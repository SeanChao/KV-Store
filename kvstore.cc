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

KVStore::~KVStore() { memTable.release(); }

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
    std::clog << "+ " << key << " " << std::string(s, 0, 50) << std::endl;
    memTable->put(key, s);
    memTableSize += getDataSize(s.length());
    if (verbose) {
        // std::clog << "put " << key << '\t' << s << "  done.\n";
        // std::clog << *memTable << '\n';
    }
    // if the size memTable reaches the threshold, then calls buildSsTable and
    // resets memTable
    if (this->memTableSize >= MEM_TABLE_SIZE_MAX) {
        convertMemTable();
        resetMemTable();
        // std::clog << *memTable << std::endl;
    }
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 *
 * Looks for key in memTable first, and then in SsTables
 */
std::string KVStore::get(uint64_t key) {
    std::clog << "? " << key << std::endl;
    // looks for key in memTable
    std::string *strPointer = memTable->get(key);
    if (strPointer) {
        std::clog << "\t[m]->" << std::string(*strPointer, 0, 40) << std::endl;
        return *strPointer;
    }
    // looks for key in SsTables using indexTable
    uint64_t offset = 0;
    int count = findIndexedKey(key, &offset);
    bool found = count != -1;
    // calculate coresponding ssTable level and id
    // int back = count;
    int level = 0;
    while (count - fileNum[level] >= 0) {
        count -= fileNum[level];
        level++;
    }
    int fileId = count;
    if (found) {
        // reads value on disk according to offest
        std::clog << "\t@" << level << "-" << fileId << std::endl;
        return readPair(resolvePath(level, fileId), offset);
    }
    return "";
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 *
 * First, looks up for key in memTable and then in ssTable.
 * If the entry is in memTable, just remove it from memTable. If the entry is in
 * an ssTable, add an entry with the same key but with empty string to lazy
 * delete the entry.
 * If it's not found, the function returns false.
 */
bool KVStore::del(uint64_t key) {
    std::clog << "- " << key << std::endl;
    std::string val = get(key);
    bool exists = false;
    std::shared_ptr<std::string> strVal(new std::string);

    uint64_t offset = 0;
    // exists in some ssTable
    if (findIndexedKey(key, &offset) != -1) {
        if (val != "") {
            put(key, "");
            exists = true;
        }else {
            exists = false;
        }
        // std::clog << "\tindexed" << std::endl;
    } else if (memTable->remove(key, strVal)) {
        this->memTableSize -= getDataSize((*strVal).length());
        exists = true;
        std::clog << "\tin mem" << std::endl;
    }
    if (!exists) std::clog << "x" << std::endl;
    return exists;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    // reset memTable
    resetMemTable();
    // Removes all existing ss-table
    int lv = 0;
    std::string level = resolvePath(lv);
    while (std::filesystem::exists(level)) {
        // std::clog << "remove " << level << std::endl;
        uintmax_t count = std::filesystem::remove_all(level);
        std::clog << "removed " << count << " file or directories" << std::endl;
        level = resolvePath(++lv);
        // std::clog << level << std::endl;
    }
    level = resolvePath(-1);
    if (std::filesystem::exists(level)) std::filesystem::remove_all(level);
    indexTableList.clear();
    fileNum.clear();
    fileNum.push_back(0);
}

void KVStore::convertMemTable() {
    // get pointer to the head of linked list from SkipList. Beware that the
    // last non-nullptr pointer would be the tail, which contains no meaningful
    // data
    std::shared_ptr<typename SkipList<uint64_t, std::string>::Node> p =
        memTable->exportData();
    // while ((p = p->succ) && memTable->valid(p)) std::clog << p->key << "\n";
    std::string lv = resolvePath(0);
    std::string filename = resolvePath(0, 0);
    std::filesystem::create_directories(lv);
    renameLevel(0, 0);
    std::fstream fs(filename, std::ios::out | std::ios::binary);
    // prepares the cache for index data
    std::vector<Index> indexTable;
    uint64_t offset = 0;
    // writes the data segment
    while ((p = p->succ) && memTable->valid(p)) {
        // std::clog << "write key = " << p->key << std::endl;
        // convert to char * on demand
        uint64_t strLen = p->val.length();
        time_t writeTime = time(nullptr);
        Entry e(p->key, writeTime, strLen, p->val);
        writeEntry(fs, e);
        // caches index data
        indexTable.push_back(Index(p->key, offset));
        offset += sizeof(p->key) + sizeof(writeTime) + sizeof(strLen) +
                  p->val.length();
    }
    // writes index data to file
    for (auto i : indexTable) {
        // std::clog << "write key: " << i.key << " offset: " << i.offset
        //   << " tellp: " << fs.tellp() << std::endl;
        fs.write(reinterpret_cast<char *>(&i.key), sizeof(i.key));
        fs.write(reinterpret_cast<char *>(&i.offset), sizeof(i.offset));
    }
    // writes meta data
    // index of indexTable
    // std::clog << "Meta: offset = " << std::hex << "0x" << offset << std::dec
    //   << std::endl;
    fs.write(reinterpret_cast<char *>(&offset), sizeof(offset));
    indexTableList.insert(indexTableList.begin(), indexTable);
    fs.close();
    renameLevel(0, 1);
    std::clog << "memTable -> " << filename << std::endl;
    // update state
    fileNum[0]++;
    // compaction if the number of files in level 0 is more than 2
    if (fileNum[0] > 2) compaction();
}

void KVStore::loadSsTable() {
    // checks existing ss-table
    int lv = 0;
    int count = 0;
    std::string level = resolvePath(lv);
    while (std::filesystem::exists(level)) {
        // std::string filename = "sstable-" + std::to_string(count);
        // std::clog << "see lv " << lv << std::endl;
        std::string filename = resolvePath(lv, count);
        if ((int)fileNum.size() < lv + 1) fileNum.push_back(0);
        while (std::filesystem::exists(filename)) {
            // std::clog << "see file " << filename << std::endl;
            // parse the file
            readSsTable(filename);
            fileNum[lv]++;
            filename = resolvePath(lv, ++count);
        }
        count = 0;
        level = resolvePath(++lv);
    }
}

void KVStore::readSsTable(std::string path) {
    // load index table into indexTableList
    std::ifstream fs(path, std::ios::binary);
    // std::clog << "read ss table from " << path << std::endl;
    // reads the offest of the begining of the index part
    if (!fs.is_open()) {
        // std::clog << "Error open file " << path << std::endl;
    }
    fs.seekg(-8, std::ios::end);
    uint64_t offset = 0;
    fs.read(reinterpret_cast<char *>(&offset), sizeof(offset));
    // std::clog << "read: get offset of index table at " << std::hex << offset
    //           << std::dec << std::endl;
    // reads keys and indices and saves them in indexTable
    fs.seekg(offset);
    std::vector<Index> indexTable;
    while (!fs.eof()) {
        uint64_t key = 0;
        uint64_t off = 0;
        // int64_t time = 0;
        fs.read(reinterpret_cast<char *>(&key), sizeof(key));
        // fs.read(reinterpret_cast<char *>(&time), sizeof(time));
        fs.read(reinterpret_cast<char *>(&off), sizeof(off));
        if (fs.gcount() == 0)
            break;  // if fs comes to meta data then break, this condition may
                    // fail if the meta data changes
        // std::clog << "read key: " << key << std::hex << " @0x" << off << " "
        // << std::dec << fs.gcount() << std::endl;
        indexTable.push_back(Index(key, off));
    }
    fs.close();
    // for(auto i: indexTable) {
    //     std::clog << "key " << i.key << "offset "
    // }
    indexTableList.push_back(indexTable);
}

std::vector<Pair> KVStore::readSsTable(int level, int id) {
    std::vector<Pair> table;
    std::string path = resolvePath(level, id);
    std::ifstream fs(path, std::ios::binary);
    // std::clog << "read ss table from " << path << std::endl;
    if (!fs.is_open()) {
        // std::clog << "error read sstable " << path << std::endl;
        return table;
    }
    fs.seekg(-8, std::ios::end);
    uint64_t offset = 0;
    fs.read(reinterpret_cast<char *>(&offset), sizeof(offset));
    // std::clog << "read: offset of index table at " << std::hex << offset
    //           << std::dec << std::endl;

    fs.seekg(std::ios::beg);
    while (fs.tellg() < (int64_t)offset) {
        // std::clog << "fs pos: " << fs.tellg() << std::endl;
        uint64_t key = 0;
        uint64_t len = 0;
        time_t time = 0;
        fs.read(reinterpret_cast<char *>(&key), sizeof(key));
        fs.read(reinterpret_cast<char *>(&time), sizeof(time));
        fs.read(reinterpret_cast<char *>(&len), sizeof(len));
        char *tmp = new char[len + 1];
        fs.read(tmp, len);
        tmp[len] = '\0';
        std::string val(tmp);
        delete[] tmp;
        table.push_back(Pair(key, time, val));
    }
    fs.close();
    // for (auto i : table)
    // std::clog << "key " << i.key << "\tval " << std::string(i.val, 0, 50)
    // << std::endl;

    return table;
}

void KVStore::writeEntry(std::fstream &fs, Entry &e) {
    if (!fs.is_open()) {
        // std::clog << "Error open file" << std::endl;
        return;
    }
    fs.write(reinterpret_cast<char *>(&e.key), sizeof(e.key));
    fs.write(reinterpret_cast<char *>(&e.timestamp), sizeof(e.timestamp));
    fs.write(reinterpret_cast<char *>(&e.len), sizeof(e.len));
    fs.write(e.str.c_str(), e.len);  // write val
}

int KVStore::findIndexedKey(uint64_t key, uint64_t *offsetDst) const {
    int count = 0;
    bool found = false;
    uint64_t offset = 0;
    for (auto table : indexTableList) {
        if (found) break;
        // binary searches in an index table
        int l = 0;
        int r = table.size() - 1;
        while (l <= r) {
            int mid = (l + r) / 2;
            if (table[mid].key == key) {
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
    if (found && offsetDst != nullptr) *offsetDst = offset;
    return found ? count : -1;
}

std::string KVStore::resolvePath(int level, int id) const {
    std::filesystem::path lv(resolvePath(level));
    std::string filename = "sstable-" + std::to_string(id);
    return std::filesystem::path(lv / filename).string();
}

std::string KVStore::resolvePath(int level) const {
    std::filesystem::path root(this->dir);
    std::string lv = "level-" + std::to_string(level);
    if (level == -1) lv = "tmp";
    return std::filesystem::path(root / lv).string();
}

std::string KVStore::resolvePath(Location &l) const {
    return resolvePath(l.level, l.id);
}

std::string KVStore::readPair(std::string path, uint64_t offset) {
    std::ifstream fs(path, std::ios::binary);
    uint64_t len = 0;
    // 16 = key + time
    fs.seekg(offset + 16);
    fs.read(reinterpret_cast<char *>(&len), sizeof(len));
    char *tmp = new char[len + 1];
    fs.read(tmp, len);
    fs.close();
    tmp[len] = '\0';
    std::string val(tmp);
    delete[] tmp;
    // std::clog << "read " << len << " byte: " << val << std::endl;
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

void KVStore::writeSsTable(std::vector<Pair> table, Location loc, bool tmp) {
    std::string path = resolvePath(loc.level, loc.id);
    std::string folder = resolvePath(loc.level);
    if (!std::filesystem::exists(folder))
        std::filesystem::create_directories(folder);
    std::fstream fs(path, std::ios::out | std::ios::binary);
    if (!fs.is_open()) {
        // std::clog << "[writeSsTable] Failed to open sstable file " << path
        // << std::endl;
        return;
    }
    // prepares the cache for index data
    std::vector<Index> indexTable;
    uint64_t offset = 0;
    // writes the data segment
    for (auto &p : table) {
        uint64_t strLen = p.val.length();
        time_t writeTime = time(nullptr);
        Entry e(p.key, writeTime, strLen, p.val);
        writeEntry(fs, e);
        // caches index data
        indexTable.push_back(Index(p.key, offset));
        offset +=
            sizeof(p.key) + sizeof(writeTime) + sizeof(strLen) + p.val.length();
    }
    // writes index data to file
    for (auto i : indexTable) {
        // std::clog << "write key = " << i.key << " offset = " << i.offset
        // << std::endl;
        fs.write(reinterpret_cast<char *>(&i.key), sizeof(i.key));
        fs.write(reinterpret_cast<char *>(&i.offset), sizeof(i.offset));
    }
    // writes meta data
    // index of indexTable
    // std::clog << "Meta: offset = " << std::hex << "0x" << offset << std::dec
    // << std::endl;
    fs.write(reinterpret_cast<char *>(&offset), sizeof(offset));
    fs.close();
    // update state
    indexTableList.insert(indexTableList.begin() + getIndex(loc), indexTable);
    if ((int)fileNum.size() <= loc.level) fileNum.push_back(0);
    if (loc.level >= 0) fileNum[loc.level]++;
    std::clog << "write ss table done -> " << path << std::endl;
}

void KVStore::compaction(int level) {
    std::clog << "run compaction on level " << level << std::endl;
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
        // std::clog << "[lv0]compaction range " << min << " " << max <<
        // std::endl;
        id.push_back(Location(0, 2));
        id.push_back(Location(0, 1));
        id.push_back(Location(0, 0));
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
    // looks for key range overlapped files
    int delFiles = 0;
    for (int i = 0; fileNum.size() > (size_t)nextLv && i < fileNum[nextLv];
         i++) {
        int tabId = getIndex(nextLv, i);
        uint64_t tMin = indexTableList[tabId].front().key;
        uint64_t tMax = indexTableList[tabId].back().key;
        if (!(tMax < min || max < tMin)) {
            id.push_back(Location(nextLv, i));
            delFiles++;
            // if (nextLvPos == -1) nextLvPos = i;
        }
        if (tMax < min) nextLvPos = i + 1;
    }
    // merge
    std::vector<Pair> all;
    for (size_t i = 0; i < id.size(); i++) {
        // reads ssTable and build pair vector
        std::vector<Pair> t = readSsTable(id[i].level, id[i].id);
        all = merge(all, t);
    }
    // std::clog << "Merged all data: " << std::endl;
    // for (auto &i : all)
    // std::clog << "\t" << i.key << ":\t" << std::string(i.val, 0, 50)
    // << std::endl;
    // update state: removes indexTable from memory, updates fileNum
    for (int i = (int)id.size() - 1; i >= 0; i--) {
        indexTableList.erase(indexTableList.begin() + getIndex(id[i]));
        fileNum[id[i].level]--;
        if (!std::filesystem::remove(resolvePath(id[i])))
            std::clog << "error deleting " << resolvePath(id[i]) << std::endl;
    }
    // slice merged data and write to disk
    // program state is updated in call to writeSsTable
    uint64_t size = 0;
    std::vector<Pair> tmp;
    if (nextLvPos == -1) nextLvPos = 0;
    // renames all file in nextLv to avoid rename hazard
    renameLevel(nextLv, 0);
    for (auto &i : all) {
        size += i.val.length() + DATA_CONST_SIZE;
        tmp.push_back(i);
        if (size >= MEM_TABLE_SIZE_MAX || i.key == all.back().key) {
            writeSsTable(tmp, Location(nextLv, nextLvPos++), true);
            size = 0;
            tmp.clear();
        }
    }
    renameLevel(nextLv, 1);
    // call compaction on next level if necessary
    if (fileNum[nextLv] > levelSizeLim(nextLv)) compaction(nextLv);
}

std::vector<Pair> KVStore::merge(std::vector<Pair> a, std::vector<Pair> b) {
    std::vector<Pair> tmp;
    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        if (a[i].key < b[j].key)
            tmp.push_back(a[i++]);
        else if (a[i].key > b[j].key)
            tmp.push_back(b[j++]);
        else {
            // two entries with the same key, keeps the one with newer timestamp
            Pair p = a[i].time > b[j].time ? a[i] : b[j];
            tmp.push_back(p);
            i++;
            j++;
        }
    }
    while (i < a.size()) {
        tmp.push_back(a[i++]);
    }
    while (j < b.size()) {
        tmp.push_back(b[j++]);
    }
    // std::clog << "tmp merged: " << std::endl;
    // for (auto i : tmp) {
    //     std::clog << i.key << " " << i.val << std::endl;
    // }
    return tmp;
}

void KVStore::renameLevel(int level, int mode) {
    if (mode == 0) {
        int id = 0;
        if (!std::filesystem::exists(resolvePath(-1)))
            std::filesystem::create_directories(resolvePath(-1));
        while (std::filesystem::exists(resolvePath(level, id))) {
            std::filesystem::rename(resolvePath(level, id),
                                    resolvePath(-1, id));
            std::clog << "rename " << resolvePath(level, id) << "->"
                      << resolvePath(-1, id) << std::endl;
            id++;
        }
    } else if (mode == 1) {
        int tmp = 0;
        int id = 0;
        while (std::filesystem::exists(resolvePath(-1, tmp))) {
            while (std::filesystem::exists(resolvePath(level, id))) id++;
            std::filesystem::rename(resolvePath(-1, tmp++),
                                    resolvePath(level, id++));
            std::clog << "rename " << resolvePath(-1, tmp - 1) << "->"
                      << resolvePath(level, id - 1) << std::endl;
        }
    } else {
        std::clog << "error no rename with mode " << mode << std::endl;
    }
}

int KVStore::getIndex(int level, int id) const {
    int index = 0;
    for (size_t i = 0; i < fileNum.size() && i < (size_t)level; i++) {
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
