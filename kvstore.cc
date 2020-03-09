#include "kvstore.h"
#include <iostream>
#include <string>
#include "skiplist.h"

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir), verbose(false) {
    memTable = std::unique_ptr<SkipList<uint64_t, std::string>>(
        new SkipList<uint64_t, std::string>());
    std::cerr << "BUILD" << '\n';
}

KVStore::~KVStore() {}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
    memTable->put(key, s);
    if (verbose) {
        std::cerr << "put " << key << '\t' << s << "  done.\n";
        std::cerr << *memTable << '\n';
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    if (verbose) std::cerr << "try get " << key;
    std::string *strPointer = memTable->get(key);
    if (verbose && strPointer)
        std::cerr << " -> " << *strPointer << '\n';
    else if (verbose)
        std::cerr << " NOT FOUND.\n";
    if (strPointer) return *strPointer;
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
    return memTable->remove(key);
    // return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {}
