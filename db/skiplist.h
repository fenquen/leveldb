// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {
    class Arena;

    template<typename Key, class Comparator>
    class SkipList {
    private:
        struct Node;
    public:
        // Create a new SkipList object that will use "cmp" for comparing keys,
        // and will allocate memory using "*arena".  Objects allocated in the arena
        // must remain allocated for the lifetime of the skiplist object.
        explicit SkipList(Comparator cmp, Arena *arena);

        SkipList(const SkipList &) = delete;

        SkipList &operator=(const SkipList &) = delete;

        // Insert key into the list.
        // REQUIRES: nothing that compares equal to key is currently in the list.
        void Insert(const Key &key);

        // Returns true iff an entry that compares equal to key is in the list.
        bool Contains(const Key &key) const;

        // Iteration over the contents of a skip list
        class Iterator {
        public:
            // Initialize an iterator over the specified list.
            // The returned iterator is not valid.
            explicit Iterator(const SkipList *list);

            // true if the iterator is positioned at a valid node.
            bool Valid() const;

            // Returns the key at the current position.
            // REQUIRES: Valid()
            const Key &key() const;

            // Advances to the next position.
            // REQUIRES: Valid()
            void Next();

            // Advances to the previous position.
            // REQUIRES: Valid()
            void Prev();

            // Advance to the first entry with a key >= target
            void Seek(const Key &target);

            // Position at the first entry in list.
            // Final state of iterator is Valid() iff list is not empty.
            void SeekToFirst();

            // Position at the last entry in list.
            // Final state of iterator is Valid() iff list is not empty.
            void SeekToLast();

        private:
            const SkipList *skipList_;
            Node *node_;
            // Intentionally copyable
        };

    private:
        enum {
            kMaxHeight = 12
        };

        inline int GetMaxHeight() const {
            return maxHeight.load(std::memory_order_relaxed);
        }

        Node *NewNode(const Key &key, int height);

        int RandomHeight();

        bool Equal(const Key &a, const Key &b) const { return (comparator(a, b) == 0); }

        // Return true if key is greater than the data stored in "n"
        bool KeyIsAfterNode(const Key &key, Node *n) const;

        // Return the earliest node that comes at or after key.
        // Return nullptr if there is no such node.
        //
        // If prev is non-null, fills prev[level] with pointer to previous
        // node at "level" for every level in [0..maxHeight-1].
        Node *FindGreaterOrEqual(const Key &key, Node **prev) const;

        // Return the latest node with a key < key.
        // Return head if there is no such node.
        Node *FindLessThan(const Key &key) const;

        // Return the last node in the list.
        // Return head if list is empty.
        Node *FindLast() const;

        // Immutable after construction
        Comparator const comparator;
        Arena *const arena;  // Arena used for allocations of nodes

        // dummy用途
        Node *const head;

        // Modified only by Insert().  Read racily by readers, but stale
        // values are ok.
        std::atomic<int> maxHeight;  // Height of the entire list

        // Read/written only by Insert().
        Random random;
    };

    // implementation details follow
    template<typename Key, class Comparator>
    struct SkipList<Key, Comparator>::Node {
        explicit Node(const Key &key) : key(key) {

        }

        Key const key;

        // 得到了该level上的next
        // Accessors/mutators for links, Wrapped in methods so we can add the appropriate barriers as necessary.
        Node *Next(int level) {
            assert(level >= 0);

            // Use an 'acquire load' so that we observe a fully initialized version of the returned Node.
            return next_[level].load(std::memory_order_acquire);
        }

        void SetNext(int n, Node *nextNode) {
            assert(n >= 0);
            // Use a 'release store' so that anybody who reads through this
            // pointer observes a fully initialized version of the inserted node.
            next_[n].store(nextNode, std::memory_order_release);
        }

        // No-barrier variants that can be safely used in a few locations.
        Node *NoBarrier_Next(int n) {
            assert(n >= 0);
            return next_[n].load(std::memory_order_relaxed);
        }

        void NoBarrier_SetNext(int level, Node *x) {
            assert(level >= 0);
            next_[level].store(x, std::memory_order_relaxed);
        }

    private:
        // 这边的[1]其实不是真的只有这么点 NewNode()有相应说明
        // Array of length equal to the node height.  next_[0] is lowest level link
        std::atomic<Node *> next_[1];
    };

    template<typename Key, class Comparator>
    typename SkipList<Key, Comparator>::Node *
    SkipList<Key, Comparator>::NewNode(const Key &key, int height) {
        char *const node_memory = arena->AllocateAligned(sizeof(Node) + sizeof(std::atomic<Node *>) * (height - 1));
        return new(node_memory) Node(key);
    }

    template<typename Key, class Comparator>
    inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList *list) {
        skipList_ = list;
        node_ = nullptr;
    }

    template<typename Key, class Comparator>
    inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
        return node_ != nullptr;
    }

    template<typename Key, class Comparator>
    inline const Key &SkipList<Key, Comparator>::Iterator::key() const {
        assert(Valid());
        return node_->key;
    }

    template<typename Key, class Comparator>
    inline void SkipList<Key, Comparator>::Iterator::Next() {
        assert(Valid());
        node_ = node_->Next(0);
    }

    template<typename Key, class Comparator>
    inline void SkipList<Key, Comparator>::Iterator::Prev() {
        // Instead of using explicit "prev" links, we just search for the
        // last node that falls before key.
        assert(Valid());
        node_ = skipList_->FindLessThan(node_->key);
        if (node_ == skipList_->head) {
            node_ = nullptr;
        }
    }

    template<typename Key, class Comparator>
    inline void SkipList<Key, Comparator>::Iterator::Seek(const Key &target) {
        node_ = skipList_->FindGreaterOrEqual(target, nullptr);
    }

    template<typename Key, class Comparator>
    inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
        node_ = skipList_->head->Next(0);
    }

    template<typename Key, class Comparator>
    inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
        node_ = skipList_->FindLast();
        if (node_ == skipList_->head) {
            node_ = nullptr;
        }
    }

    template<typename Key, class Comparator>
    int SkipList<Key, Comparator>::RandomHeight() {
        // Increase height with probability 1 in kBranching
        static const unsigned int kBranching = 4;

        int height = 1;
        while (height < kMaxHeight && ((random.Next() % kBranching) == 0)) {
            height++;
        }

        assert(height > 0);
        assert(height <= kMaxHeight);

        return height;
    }

    template<typename Key, class Comparator>
    bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key &key, Node *node) const {
        // null node is considered infinite
        return (node != nullptr) && (comparator(node->key, key) < 0);
    }

    template<typename Key, class Comparator>
    typename SkipList<Key, Comparator>::Node *
    SkipList<Key, Comparator>::FindGreaterOrEqual(const Key &key, Node **prevNodeArr) const {
        Node *node = head;
        int level = GetMaxHeight() - 1;

        while (true) {
            Node *nextNode = node->Next(level);

            // node在key前边,说明node还是不够大需要继续next()
            if (KeyIsAfterNode(key, nextNode)) {
                // Keep searching in this list
                node = nextNode;
                continue;
            }

            // 当前的node是正好要比key小的 是它的prev
            if (prevNodeArr != nullptr) {
                prevNodeArr[level] = node;
            }

            if (level == 0) {
                return nextNode; // 那个正好比key大的
            }

            // Switch to nextNode list
            level--;
        }
    }

    template<typename Key, class Comparator>
    typename SkipList<Key, Comparator>::Node *
    SkipList<Key, Comparator>::FindLessThan(const Key &key) const {
        Node *x = head;
        int level = GetMaxHeight() - 1;
        while (true) {
            assert(x == head || comparator(x->key, key) < 0);
            Node *next = x->Next(level);
            if (next == nullptr || comparator(next->key, key) >= 0) {
                if (level == 0) {
                    return x;
                } else {
                    // Switch to next list
                    level--;
                }
            } else {
                x = next;
            }
        }
    }

    template<typename Key, class Comparator>
    typename SkipList<Key, Comparator>::Node *
    SkipList<Key, Comparator>::FindLast() const {
        Node *node = head;
        int level = GetMaxHeight() - 1;
        while (true) {
            Node *next = node->Next(level);
            if (next == nullptr) {
                if (level == 0) {
                    return node;
                }

                // switch to next list
                level--;
            } else {
                node = next;
            }
        }
    }

    template<typename Key, class Comparator>
    SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena *arena)
            : comparator(cmp),
              arena(arena),
              head(NewNode(0 /* any key will do */, kMaxHeight)),
              maxHeight(1),
              random(0xdeadbeef) {
        for (int i = 0; i < kMaxHeight; i++) {
            head->SetNext(i, nullptr);
        }
    }

    template<typename Key, class Comparator>
    void SkipList<Key, Comparator>::Insert(const Key &key) {
        // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
        // here since Insert() is externally synchronized.
        Node *prev[kMaxHeight];

        // 得到的该node是level0上刚好要比该key大的
        Node *node = FindGreaterOrEqual(key, prev);

        // Our data structure does not allow duplicate insertion
        assert(node == nullptr || !Equal(key, node->key));

        int randomHeight = RandomHeight();
        if (randomHeight > GetMaxHeight()) {
            // 填补多的height
            for (int a = GetMaxHeight(); a < randomHeight; a++) {
                prev[a] = head;
            }

            // It is ok to mutate maxHeight without any synchronization with concurrent readers.
            //
            // A concurrent reader that observes the new value of maxHeight will see either the old value of
            // new level pointers from head (nullptr), or a new value set in the loop below.
            //
            // 第1种的话 the reader will 立即 drop to the next level 因为 nullptr is 无限大的  after all keys.
            //
            // in the latter case the reader will use the new node
            maxHeight.store(randomHeight, std::memory_order_relaxed);
        }

        node = NewNode(key, randomHeight);

        // 和普通链表1样的insert
        for (int i = 0; i < randomHeight; i++) {
            // NoBarrier_SetNext() suffices since we will add a barrier when
            // we publish a pointer to "node" in prev[i].
            node->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
            prev[i]->SetNext(i, node);
        }
    }

    template<typename Key, class Comparator>
    bool SkipList<Key, Comparator>::Contains(const Key &key) const {
        Node *x = FindGreaterOrEqual(key, nullptr);
        if (x != nullptr && Equal(key, x->key)) {
            return true;
        }

        return false;
    }

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_