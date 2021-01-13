#pragma once

#ifdef USE_LIBPMEM
#include <libpmem.h>
#endif

#include <libpmem.h>
#include <atomic>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "../include/db.hpp"

using std::atomic;
using std::string;
using std::vector;

typedef uint32_t HASH_VALUE;
typedef uint16_t VALUE_LEN_TYPE;
typedef uint32_t KEY_INDEX_TYPE;
typedef uint32_t BLOCK_INDEX_TYPE;
typedef long long LL;

static const uint8_t KEY_LEN = 16;
static const uint8_t KEY_STORE_VALUE_LINK_OFFSET =
    KEY_LEN + sizeof(VALUE_LEN_TYPE);
static const uint8_t KEY_STORE_LEN =
    KEY_STORE_VALUE_LINK_OFFSET + sizeof(BLOCK_INDEX_TYPE) + 2;
static const uint32_t KV_NUM_MAX = 16 * 24 * 1024 * 1024 * 0.60;
static const uint32_t HASH_MAP_SIZE = 100000000;
static const uint64_t FILE_SIZE = 68719476736UL;
static const uint8_t VALUE_BLOCK_LEN = 32;
static const uint16_t VALUE_MAX_LEN = 1024;

static const int32_t THREAD_NUM = 17;
static const uint64_t VALUE_PART_SIZE =
    FILE_SIZE - (uint64_t)KV_NUM_MAX * KEY_STORE_LEN;
static const uint32_t VALUE_PART_FIX_NUM =
    VALUE_PART_SIZE * 0.80 / THREAD_NUM / VALUE_BLOCK_LEN;

thread_local int tid = -1;
atomic<int32_t> curTid = {0};
thread_local char valueBuff[VALUE_MAX_LEN];
thread_local char keyBuff[KEY_STORE_LEN];

static const uint32_t KEY_PART_FIX_NUM = KV_NUM_MAX * 0.90 / THREAD_NUM;

// log
thread_local int find_times = 0;
thread_local int wt = 0;
thread_local int rt = 0;

HASH_VALUE DJBHash(const char* _str) {
  unsigned int hash = 5381;
  for (unsigned int i = 0; i < 16; ++_str, ++i) {
    hash = ((hash << 5) + hash) + (*_str);
  }
  return hash;
}

uint32_t default_hash(const char* str) {
  size_t len = 16;
  uint32_t hash = 0;
  uint32_t DEFAULT_HASH_SEED = 5381;
  while (len--) {
    hash = hash * DEFAULT_HASH_SEED + (*str++);
  }
  return (hash & 0x7FFFFFFFU);
}

class Entry {
 public:
  Entry() : head(UINT32_MAX){};
  ~Entry() = default;

  KEY_INDEX_TYPE getHead() const {
    return this->head.load(std::memory_order_relaxed);
  }
  // set head and return the old one
  KEY_INDEX_TYPE setHead(const uint32_t sn) {
    return this->head.exchange(sn, std::memory_order_relaxed);
  }

 public:
  std::atomic<KEY_INDEX_TYPE> head;
};

static const int lf_span = 43;

typedef uint32_t LocalFree[lf_span * THREAD_NUM];
typedef vector<vector<BLOCK_INDEX_TYPE>> LocalQueue;

typedef uint32_t LocalKey[lf_span * THREAD_NUM];

class FreeList {
 public:
  FreeList(int _maxSize, int _blockSize) {
    if (_blockSize < 32) {
      _blockSize = 32;
    }
    if (_blockSize > 128) {
      _blockSize = 128;
    }
    // exists redundant entries, listHead[0] stores large free space when
    // recovery
    int listNum = ceil((double)_maxSize / _blockSize) + 1;
    for (auto& lq : localQueues) {
      lq.resize(listNum);
    }
  };

  void setLocalFree(BLOCK_INDEX_TYPE _base) {
    for (int i = 0; i < THREAD_NUM; ++i) {
      this->localFrees[i * lf_span] = _base + (uint32_t)i * VALUE_PART_FIX_NUM;
      this->localFrees[i * lf_span + 1] =
          this->localFrees[i * lf_span] + VALUE_PART_FIX_NUM - 1;
    }
  }

  // recover free list from pmem
  void recovery(char* memBase){

  };
  // push one free block
  void push(int _size, BLOCK_INDEX_TYPE _index) {
    if (_size < 3) return;

    if (tid >= 0 && tid < THREAD_NUM) {
      this->localQueues[tid][_size].emplace_back(_index);
    }
  };

  // try to pop one appropriate free block
  bool pop(int _size, BLOCK_INDEX_TYPE* _index) {
    if (tid >= 0 && tid < THREAD_NUM &&
        this->localFrees[tid * lf_span + 1] - this->localFrees[tid * lf_span] >=
            (uint32_t)_size) {
      *_index = this->localFrees[tid * lf_span];
      this->localFrees[tid * lf_span] += _size;
      return true;
    } else {
      bool found = false;
      if (tid >= 0 && tid < THREAD_NUM) {
        found = !this->localQueues[tid][_size].empty();
        if (found) {
          *_index = this->localQueues[tid][_size].back();
          this->localQueues[tid][_size].pop_back();
        }

        if (!found) {
          for (size_t i = _size + 3; i < this->localQueues[tid].size(); ++i) {
            found = !this->localQueues[tid][i].empty();
            if (found) {
              *_index = this->localQueues[tid][i].back();
              this->localQueues[tid][i].pop_back();
              this->localQueues[tid][i - _size].emplace_back(*_index + _size);
              break;
            }
          }
        }
      }

      return found;
    }
  };

  string getStatus() {
    string ret = "local free list\n";
    for (size_t i = 0; i < THREAD_NUM; ++i) {
      ret += std::to_string(i) + ": " + std::to_string(localQueues[i].size());
      ret += "\n";
    }
    return ret;
  }

 private:
  LocalFree localFrees;
  LocalQueue localQueues[THREAD_NUM];
};

class KeyPool {
 public:
  KeyPool(char* _keyBase, uint64_t _maxLen, int _step = 1)
      : keyBuffer(new char[_maxLen]),
        keyBase(_keyBase),
        pointer(new KEY_INDEX_TYPE[KV_NUM_MAX]),
        step(_step) {}
  ~KeyPool() {
    delete[] keyBuffer;
    delete[] pointer;
    pointer = nullptr;
    keyBase = nullptr;
    keyBuffer = nullptr;
  }

  void setLocalKey(KEY_INDEX_TYPE _base) {
    for (int i = 0; i < THREAD_NUM; ++i) {
      this->localKeys[i * lf_span] = _base + (uint32_t)i * KEY_PART_FIX_NUM;
      this->localKeys[i * lf_span + 1] =
          this->localKeys[i * lf_span] + KEY_PART_FIX_NUM - 1;
    }
  }

  void push(const Slice& _key, VALUE_LEN_TYPE _len,
            BLOCK_INDEX_TYPE _blockIndex, Entry* _entry) {
    KEY_INDEX_TYPE index;
    if (tid >= 0 && tid < THREAD_NUM &&
        this->localKeys[tid * lf_span + 1] > this->localKeys[tid * lf_span]) {
      index = this->localKeys[tid * lf_span];
      ++this->localKeys[tid * lf_span];
    } else {
      index = currentIndex.fetch_add(1);
    }

    auto oldHead = _entry->setHead(index);
    pointer[index] = oldHead;
    char* base = keyBuffer + static_cast<uint64_t>(index) * KEY_STORE_LEN;

    memcpy(base, _key.data(), KEY_LEN);
    memcpy(base + KEY_LEN, &_len, sizeof(VALUE_LEN_TYPE));
    memcpy(base + KEY_STORE_VALUE_LINK_OFFSET, &_blockIndex,
           sizeof(BLOCK_INDEX_TYPE));
    pmem_memcpy_persist(keyBase + static_cast<uint64_t>(index) * KEY_STORE_LEN,
                        base, KEY_STORE_LEN);
  }
  KEY_INDEX_TYPE find(const Slice& _key, KEY_INDEX_TYPE _index) {
    KEY_INDEX_TYPE re = UINT32_MAX;
    while (_index != UINT32_MAX) {
      re = _index;
      char* temp = next(_index);
      if (memcmp(temp, _key.data(), KEY_LEN) == 0) {
        return re;
      }
    }
    return UINT32_MAX;
  }
  VALUE_LEN_TYPE getValLen(KEY_INDEX_TYPE _index) {
    return *(VALUE_LEN_TYPE*)(keyBuffer +
                              static_cast<uint64_t>(_index) * KEY_STORE_LEN +
                              KEY_LEN);
  }
  BLOCK_INDEX_TYPE getValIndex(KEY_INDEX_TYPE _index) {
    return *(BLOCK_INDEX_TYPE*)(keyBuffer +
                                static_cast<uint64_t>(_index) * KEY_STORE_LEN +
                                KEY_STORE_VALUE_LINK_OFFSET);
  }
  char* next(KEY_INDEX_TYPE& _index) {
    char* key = keyBuffer + static_cast<uint64_t>(_index) * KEY_STORE_LEN;
    _index = pointer[_index];
    return key;
  }

  void update(KEY_INDEX_TYPE _keyIndex, VALUE_LEN_TYPE _len,
              BLOCK_INDEX_TYPE blockIndex) {
    char* base = keyBuffer + static_cast<uint64_t>(_keyIndex) * KEY_STORE_LEN;
    memcpy(base + KEY_LEN, &_len, sizeof(VALUE_LEN_TYPE));
    memcpy(base + KEY_STORE_VALUE_LINK_OFFSET, &blockIndex,
           sizeof(BLOCK_INDEX_TYPE));
    pmem_memcpy_persist(
        keyBase + static_cast<uint64_t>(_keyIndex) * KEY_STORE_LEN, base,
        KEY_STORE_LEN);
  }
  BLOCK_INDEX_TYPE recovery(KEY_INDEX_TYPE _keyIndex) {
    KEY_INDEX_TYPE index = currentIndex.fetch_add(1);
    pointer[index] = _keyIndex;
    return index;
  }
  void recoveryKeyBuffer() {
    VALUE_LEN_TYPE len = *(BLOCK_INDEX_TYPE*)(keyBase + KEY_LEN);
    if (len == 0) {
      return;
    }
    memcpy(keyBuffer, keyBase, KV_NUM_MAX * KEY_STORE_LEN);
  }

  KEY_INDEX_TYPE getKeyIndex() { return currentIndex.load(); }

 public:
  std::atomic<KEY_INDEX_TYPE> currentIndex = {0};
  LocalKey localKeys;
  char* keyBuffer;
  char* keyBase;
  KEY_INDEX_TYPE* pointer;
  int step;
};

class KVStore {
 public:
  KVStore(char* _memBase) {
    this->valueBase = _memBase + (uint64_t)KV_NUM_MAX * KEY_STORE_LEN;
    this->curValueIndex = 0;
    this->freeList = new FreeList(VALUE_MAX_LEN, VALUE_BLOCK_LEN);
    this->key_pool =
        new KeyPool(_memBase, (uint64_t)KV_NUM_MAX * KEY_STORE_LEN);
  };
  virtual ~KVStore() {
    delete this->freeList;
    delete key_pool;
  }
  // set the offset of key index and value index
  void recovery() {}
  // read key and value according to the index of key
  void read(KEY_INDEX_TYPE _index, string* _value) {
    char* base =
        key_pool->keyBuffer + static_cast<uint64_t>(_index) * KEY_STORE_LEN;
    VALUE_LEN_TYPE dataLen = *(VALUE_LEN_TYPE*)(base + KEY_LEN);
    BLOCK_INDEX_TYPE index =
        *(BLOCK_INDEX_TYPE*)(base + KEY_STORE_VALUE_LINK_OFFSET);
    _value->assign(this->valueBase + (uint64_t)index * VALUE_BLOCK_LEN,
                   dataLen);
  };

  BLOCK_INDEX_TYPE getBlockIndex(const Slice& _value);

  // write kv pair to pmem
  void write(const Slice& _key, const Slice& _value, Entry* _entry) {
    VALUE_LEN_TYPE dataLen = _value.size();
    // store and flush value
    BLOCK_INDEX_TYPE bi = getBlockIndex(_value);

    pmem_memcpy_persist(this->valueBase + (uint64_t)bi * VALUE_BLOCK_LEN,
                        _value.data(), dataLen);

    // add store into key pool.
    key_pool->push(_key, dataLen, bi, _entry);
  };

  void update(const Slice& _key, const Slice& _value, KEY_INDEX_TYPE _index) {
    char* base =
        key_pool->keyBuffer + static_cast<uint64_t>(_index) * KEY_STORE_LEN;
    VALUE_LEN_TYPE dataLen = *(VALUE_LEN_TYPE*)(base + KEY_LEN);
    BLOCK_INDEX_TYPE blockIndex =
        *(BLOCK_INDEX_TYPE*)(base + KEY_STORE_VALUE_LINK_OFFSET);
    BLOCK_INDEX_TYPE newBi = getBlockIndex(_value);

    pmem_memcpy_persist(this->valueBase + (uint64_t)newBi * VALUE_BLOCK_LEN,
                        _value.data(), _value.size());

    key_pool->update(_index, _value.size(), newBi);
    erase(dataLen, blockIndex);
  }

  // erase value according to its head index
  void erase(VALUE_LEN_TYPE _dataLen, BLOCK_INDEX_TYPE _index) {
    int size = (_dataLen + VALUE_BLOCK_LEN - 1) / VALUE_BLOCK_LEN;
    this->freeList->push(size, _index);
  }

  void setValueIndex(BLOCK_INDEX_TYPE index) { curValueIndex.store(index); }
  BLOCK_INDEX_TYPE getValueIndex() { return curValueIndex.load(); }

 public:
  KeyPool* key_pool;
  FreeList* freeList;

 private:
  atomic<BLOCK_INDEX_TYPE> curValueIndex;
  char* valueBase = nullptr;
};

typedef uint32_t (*hash_func)(const char*);

class NvmEngine;
class HashMap {
 public:
  HashMap(char* _base, hash_func _hash = DJBHash);

  ~HashMap();

  Status get(const Slice& _key, std::string* _value);

  Status set(const Slice& _key, const Slice& _value);

  Status recovery(char* _base);

  void summary();

  KVStore* kvStore;

 private:
  uint32_t hashValue(const Slice& _key) const {
    return this->hash(_key.data());
  }
  Entry& entry(const uint32_t _hash) { return entries[_hash % HASH_MAP_SIZE]; }

 private:
  Entry* entries;
  hash_func hash;
};

class NvmEngine : DB {
 public:
  static FILE* LOG;

 public:
  static Status CreateOrOpen(const std::string& name, DB** dbptr,
                             FILE* log_file = nullptr);

  NvmEngine(const std::string& _name, FILE* _log_file);

  ~NvmEngine() override;

  Status Get(const Slice& _key, std::string* _value) override;

  Status Set(const Slice& _key, const Slice& _value) override;

 private:
  HashMap* hashMap;
};