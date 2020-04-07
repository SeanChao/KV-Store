#include <iostream>
#include <memory>
#include <vector>

#ifndef SKIPLIST_H
#define SKIPLIST_H
#include "common.h"
template <typename Key, typename Value>
class SkipList {
   public:
    struct Node {
        Key key;
        Value val;
        std::shared_ptr<Node> pred;
        std::shared_ptr<Node> succ;
        std::shared_ptr<Node> above;
        std::shared_ptr<Node> below;
        Node() : pred(nullptr), succ(nullptr), above(nullptr), below(nullptr) {}
        Node(Key key, Value val) : Node() {
            this->key = key;
            this->val = val;
        }
    };

    SkipList();
    bool put(Key key, Value value);
    Value* get(Key key) const;
    // removes an element according to the key, returns whether the element
    // exists. Backups a copy of the value if passing a shared_ptr
    bool remove(Key key, std::shared_ptr<Value> v = nullptr);
    // return an array of Nodes, including Key and Value
    std::shared_ptr<Node> exportData();
    // if the node is neither a head nor a tail and it's not nullptr, it's valid
    bool valid(std::shared_ptr<typename SkipList<Key, Value>::Node>) const;
    
    template <typename X, typename Y>
    friend std::ostream& operator<<(std::ostream& os, const SkipList<X, Y>& l);
    bool test() const;

   protected:
    // return the element with the same key or the nearest smaller element
    std::shared_ptr<typename SkipList<Key, Value>::Node> skipSearch(
        Key key) const;

   private:
    std::shared_ptr<Node> head;
    std::shared_ptr<Node> tail;
    unsigned level;
};

template <typename Key, typename Value>
bool SkipList<Key, Value>::test() const {
    std::shared_ptr<Node> ptr = head;
    std::shared_ptr<Node> levelHead = head;
    while (ptr->succ || levelHead->below) {
        bool upTest = ptr->above ? ptr->above->below == ptr : true;
        bool downTest = ptr->below ? ptr->below->above == ptr : true;
        bool predTest = ptr->pred ? ptr->pred->succ == ptr : true;
        bool succTest = ptr->succ ? ptr->succ->pred == ptr : true;
        if (!upTest || !downTest || !predTest || !succTest) {
            std::cout << "ERRNST: at " << ptr << " key = " << ptr->key << '\n';
        }
        if (ptr->succ)
            ptr = ptr->succ;
        else {
            if (levelHead->below) {
                levelHead = levelHead->below;
                ptr = levelHead;
            }
        }
    }
    return true;
}

template <typename Key, typename Value>
SkipList<Key, Value>::SkipList() : level(1) {
    srand(time(nullptr));
    head = std::shared_ptr<Node>(new Node());
    tail = std::shared_ptr<Node>(new Node());
    head->succ = tail;
    tail->pred = head;
}

template <typename Key, typename Value>
bool SkipList<Key, Value>::put(Key key, Value value) {
    std::shared_ptr<Node> newNode(new Node(key, value));
    std::shared_ptr<Node> ptr = skipSearch(key);  // nearest element's top level
    // if the key exists, then update its value
    if (ptr->key == key && valid(ptr)) {
        while (ptr) {
            ptr->val = value;
            ptr = ptr->below;
        }
        return true;
    }

    while (ptr->below) ptr = ptr->below;
    newNode->pred = ptr;
    newNode->succ = ptr->succ;
    ptr->succ->pred = newNode;
    ptr->succ = newNode;

    // randomly grow higher
    while (rand() & 1) {
        // find the nearest node with higher level
        while (ptr->pred && !ptr->above) ptr = ptr->pred;
        // generate the head for new higher level
        if (!ptr->above) {
            std::shared_ptr<Node> newHead(new Node());  // new head
            std::shared_ptr<Node> newTail(new Node());  // new trailing node
            newHead->below = head;
            newHead->succ = newTail;
            head->above = newHead;
            newTail->pred = newHead;
            newTail->below = tail;
            tail->above = newTail;
            head = newHead;
            tail = newTail;
        }
        ptr = ptr->above;
        // build the new level
        std::shared_ptr<Node> growNode(new Node(key, value));
        growNode->pred = ptr;
        growNode->succ = ptr->succ;
        growNode->below = newNode;
        ptr->succ->pred = growNode;
        ptr->succ = growNode;
        newNode->above = growNode;
        newNode = growNode;
    }
    return true;
}

template <typename Key, typename Value>
Value* SkipList<Key, Value>::get(Key key) const {
    std::shared_ptr<Node> temp = skipSearch(key);
    if (temp && temp->key == key) return &temp->val;
    return nullptr;
}

template <typename Key, typename Value>
bool SkipList<Key, Value>::remove(Key key, std::shared_ptr<Value> backupVal) {
    std::shared_ptr<Node> ptr = skipSearch(key);
    if (!valid(ptr) || ptr->key != key) return false;
    if (backupVal) *backupVal = ptr->val;
    while (ptr) {
        ptr->pred->succ = ptr->succ;
        ptr->succ->pred = ptr->pred;
        // if this level is empty and it's not bottom then delete it
        if (!ptr->pred->pred && !ptr->succ->succ && ptr->below) {
            if (head->succ == tail && tail->pred == head) {
                head = head->below;
                head->above = nullptr;
                tail = tail->below;
                tail->above = nullptr;
            }
        }
        ptr = ptr->below;
    }
    return true;
}

template <typename Key, typename Value>
std::ostream& operator<<(std::ostream& os, const SkipList<Key, Value>& l) {
    std::shared_ptr<typename SkipList<Key, Value>::Node> vPtr = l.head;
    while (vPtr) {
        std::shared_ptr<typename SkipList<Key, Value>::Node> hPtr = vPtr->succ;
        os << "#" << vPtr << " ";
        while (hPtr->succ) {
            os << (hPtr) << " " << hPtr->key << hPtr->val << '\t';
            hPtr = hPtr->succ;
        }
        os << "#" << hPtr << " ";
        os << '\n';
        vPtr = vPtr->below;
    }
    return os;
}

template <typename Key, typename Value>
std::shared_ptr<typename SkipList<Key, Value>::Node>
SkipList<Key, Value>::skipSearch(Key key) const {
    std::shared_ptr<Node> temp = head->succ;
    std::shared_ptr<Node> levelHead = head;  // marks the head of current level
    std::vector<Key> path;
    while (temp) {
        while ((temp->succ && temp->key <= key) || temp == levelHead) {
            temp = temp->succ;
            path.push_back(temp->key);
        }
        if (!temp->pred) {
            std::cerr << *this << '\n';
            std::cerr << "ERR with key " << key << '\n';
            std::cerr << "level head @" << levelHead << '\n';
            for (auto n = path.begin(); n != path.end(); n++) {
                std::cout << *n << '\t';
            }
            std::cout << '\n';
        }
        temp = temp->pred;

        path.push_back(temp->key);
        // find a match
        // temp->pred is to prevent visiting head's
        if (temp && temp->pred && temp->key == key && valid(temp)) {
            return temp;
        }
        // return if no lower level exists
        if (!levelHead->below) return temp;
        // go to lower level or the head of lower level
        if (temp->below) {
            temp = temp->below;
            path.push_back(temp->key);
        } else
            temp = levelHead->below;
        levelHead = levelHead->below;
        // if (!temp->below) return temp;
        // temp = temp->below;
    }
    return nullptr;
}

template <typename Key, typename Value>
bool SkipList<Key, Value>::valid(
    std::shared_ptr<typename SkipList<Key, Value>::Node> node) const {
    return node && (node->pred) && (node->succ);
}

template <typename Key, typename Value>
std::shared_ptr<typename SkipList<Key, Value>::Node>
SkipList<Key, Value>::exportData() {
    std::shared_ptr<Node> tmp = head;
    while (tmp->below) tmp = tmp->below;
    return tmp;
}

#endif  // SKIPLIST_H
