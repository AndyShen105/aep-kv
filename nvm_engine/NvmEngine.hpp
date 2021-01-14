#pragma once
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

// size of record
static const uint8_t KEY_LEN = 16;
static const uint8_t VAL_SIZE_LEN = 2;
static const uint8_t CHECK_SUM_LEN = 4;
static const uint8_t TIME_STAMP_LEN = 4;
static const uint8_t RECORD_FIX_LEN = 26;
static const uint16_t VALUE_MAX_LEN = 1024;

// offset of record
static const uint8_t KEY_OFFSET = 0;
static const uint8_t VAL_SIZE_OFFSET = KEY_LEN;
static const uint8_t TIME_STAMP_OFFSET = VAL_SIZE_OFFSET + VAL_SIZE_LEN;
static const uint8_t VALUE_OFFSET = TIME_STAMP_OFFSET + TIME_STAMP_LEN;

// aep setting
static const uint8_t BLOCK_LEN = 32;
static const uint64_t FILE_SIZE = 68719476736UL;

// hash setting
static const uint32_t KV_NUM_MAX = 16 * 24 * 1024 * 1024 * 0.60;
static const uint32_t HASH_MAP_SIZE = 100000000;

// log
thread_local int wt = 0;

HASH_VALUE DJBHash(const char* _str, size_t _size = 16) {
  unsigned int hash = 5381;
  for (unsigned int i = 0; i < _size; ++_str, ++i) {
    hash = ((hash << 5) + hash) + (*_str);
  }
  return hash;
}

class Entry {
 public:
  Entry() : head_(UINT32_MAX){};
  ~Entry() = default;

  KEY_INDEX_TYPE GetHead() const {
    return this->head_.load(std::memory_order_relaxed);
  }
  // Set head and return the old one
  KEY_INDEX_TYPE SetHead(const uint32_t _sn) {
    return this->head_.exchange(_sn, std::memory_order_relaxed);
  }

 public:
  std::atomic<KEY_INDEX_TYPE> head_;
};

class AepMemoryController {
 public:
  AepMemoryController(int _maxSize, int _blockSize){};

  void SetLocalFree(BLOCK_INDEX_TYPE _base) {}

  // recover free list from pmem
  void Recovery(char* _memBase){};
  // push one free block
  void Push(int _size, BLOCK_INDEX_TYPE _index){};

  // try to pop one appropriate free block
  bool Pop(int _size, BLOCK_INDEX_TYPE* _index) { return true; };
};

class KVStore {
 public:
  KVStore(char* _memBase) : aep_base_(_memBase) {
    this->key_buffer_ = new char[KV_NUM_MAX * KEY_LEN];
    this->next_ = new KEY_INDEX_TYPE[KV_NUM_MAX];
    this->block_index_ = new BLOCK_INDEX_TYPE[KV_NUM_MAX];
    this->val_lens_ = new VALUE_LEN_TYPE[KV_NUM_MAX];
    // TODO: ADD FREE LSIT
  };
  ~KVStore() {
    // delete this->freeList;
    delete this->next_;
    delete this->key_buffer_;
    delete this->block_index_;
  }

  // read key and value according to the index of key
  void read(KEY_INDEX_TYPE _index, string* _value) {
    BLOCK_INDEX_TYPE index = block_index_[_index];
    _value->assign(this->aep_base_ + (uint64_t)index * BLOCK_LEN + VALUE_OFFSET,
                   val_lens_[index]);
  };

  BLOCK_INDEX_TYPE GetBlockIndex(const Slice& _value);

  // Write kv pair to pmem
  void Write(const Slice& _key, const Slice& _value, Entry* _entry) {
    VALUE_LEN_TYPE dataLen = _value.size();
    // store and flush value
    BLOCK_INDEX_TYPE bi = GetBlockIndex(_value);
    size_t recordLen = RECORD_FIX_LEN + _value.size();
    char* recordBuffer = new char[recordLen];
    VALUE_LEN_TYPE len = _value.size();
    memcpy(recordBuffer, _key.data(), KEY_LEN);
    memcpy(recordBuffer + VAL_SIZE_OFFSET, &len, VAL_SIZE_LEN);
    memcpy(recordBuffer + TIME_STAMP_OFFSET, "1234", TIME_STAMP_LEN);
    memcpy(recordBuffer + VALUE_OFFSET, _value.data(), _value.size());
    HASH_VALUE checkSum = DJBHash(recordBuffer, recordLen - CHECK_SUM_LEN);
    memcpy(recordBuffer + recordLen - CHECK_SUM_LEN, &checkSum, CHECK_SUM_LEN);
    // memcpy to pmem and flush
    pmem_memcpy_persist(this->aep_base_ + (uint64_t)bi * BLOCK_LEN, _value.data(),
                        dataLen);
    delete[] recordBuffer;
    // Update key buffer in memory
    KEY_INDEX_TYPE index;
    index = current_key_index_.fetch_add(1);
    auto oldHead = _entry->SetHead(index);
    next_[index] = oldHead;
    block_index_[index] = bi;
    memcpy(key_buffer_ + index * KEY_LEN, _key.data(), KEY_LEN);
    _entry->SetHead(index);
  };

  void Update(const Slice& _key, const Slice& _value, KEY_INDEX_TYPE _index) {
    VALUE_LEN_TYPE dataLen = val_lens_[_index];
    BLOCK_INDEX_TYPE oldBlockIndex = block_index_[_index];

    BLOCK_INDEX_TYPE newBlockIndex = GetBlockIndex(_value);
    size_t recordLen = RECORD_FIX_LEN + _value.size();
    char* recordBuffer = new char[recordLen];
    VALUE_LEN_TYPE len = _value.size();
    memcpy(recordBuffer, _key.data(), KEY_LEN);
    memcpy(recordBuffer + VAL_SIZE_OFFSET, &len, VAL_SIZE_LEN);
    memcpy(recordBuffer + TIME_STAMP_OFFSET, "1234", TIME_STAMP_LEN);
    memcpy(recordBuffer + VALUE_OFFSET, _value.data(), _value.size());
    HASH_VALUE checkSum = DJBHash(recordBuffer, recordLen - CHECK_SUM_LEN);
    memcpy(recordBuffer + recordLen - CHECK_SUM_LEN, &checkSum, CHECK_SUM_LEN);
    // memcpy to pmem and flush
    pmem_memcpy_persist(this->aep_base_ + (uint64_t)newBlockIndex * BLOCK_LEN,
                        _value.data(), dataLen);
    delete[] recordBuffer;
    block_index_[_index] = newBlockIndex;
    Recycle(dataLen, oldBlockIndex);
  }

  // Recycle value according to its head index
  void Recycle(VALUE_LEN_TYPE _dataLen, BLOCK_INDEX_TYPE _index) {
    // TODO: ADD freelist
    int size = (RECORD_FIX_LEN + _dataLen + BLOCK_LEN - 1) / BLOCK_LEN;
    this->aep_memory_controller_->Push(size, _index);
  }

  KEY_INDEX_TYPE Find(const Slice& _key, KEY_INDEX_TYPE _index) {
    KEY_INDEX_TYPE re = UINT32_MAX;
    while (_index != UINT32_MAX) {
      re = _index;
      char* temp = key_buffer_ + _index * KEY_LEN;
      if (memcmp(temp, _key.data(), KEY_LEN) == 0) {
        return re;
      }
      _index = next_[_index];
    }
    return UINT32_MAX;
  }

 public:
  AepMemoryController* aep_memory_controller_;

 private:
  atomic<BLOCK_INDEX_TYPE> cur_value_index_ = {0};
  std::atomic<KEY_INDEX_TYPE> current_key_index_ = {0};
  KEY_INDEX_TYPE* next_ = nullptr;
  BLOCK_INDEX_TYPE* block_index_ = nullptr;
  VALUE_LEN_TYPE* val_lens_ = nullptr;
  char* key_buffer_ = nullptr;
  char* aep_base_ = nullptr;
};

typedef uint32_t (*hash_func)(const char*, size_t size);

class NvmEngine;
class HashMap {
 public:
  explicit HashMap(char* _base, hash_func _hash = DJBHash);

  ~HashMap();

  Status Get(const Slice& _key, std::string* _value);

  Status Set(const Slice& _key, const Slice& _value);

  Status Recovery(char* _base);

  void Summary();

  KVStore* kv_store_;

 private:
  uint32_t hash_value(const Slice& _key) const {
    return this->hash_(_key.data(), KEY_LEN);
  }
  Entry& entry(const uint32_t _hash) { return entries_[_hash % HASH_MAP_SIZE]; }

 private:
  Entry* entries_;
  hash_func hash_;
};

class NvmEngine : DB {
 public:
  static FILE* LOG;

 public:
  static Status CreateOrOpen(const std::string& _name, DB** _dbptr,
                             FILE* _log_file = nullptr);

  NvmEngine(const std::string& _name, FILE* _log_file);

  ~NvmEngine() override;

  Status Get(const Slice& _key, std::string* _value) override;

  Status Set(const Slice& _key, const Slice& _value) override;

 private:
  HashMap* hash_map_;
};