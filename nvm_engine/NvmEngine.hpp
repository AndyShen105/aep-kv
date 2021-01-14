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
typedef uint16_t VERSION_TYPE;

// size of record
static const uint8_t KEY_LEN = 16;
static const uint8_t VAL_SIZE_LEN = 2;
static const uint8_t CHECK_SUM_LEN = 4;
static const uint8_t VERSION_LEN = 2;
static const uint8_t RECORD_FIX_LEN = 24;
static const uint16_t VALUE_MAX_LEN = 1024;

// offset of record
static const uint8_t VAL_SIZE_OFFSET = 0;
static const uint8_t KEY_OFFSET = VAL_SIZE_LEN;
static const uint8_t VERSION_OFFSET = KEY_OFFSET + KEY_LEN;
static const uint8_t VALUE_OFFSET = VERSION_OFFSET + VERSION_LEN;

// aep setting
static const uint8_t BLOCK_LEN = 64;
static const uint64_t FILE_SIZE = 68719476736UL;
// static const uint64_t FILE_SIZE = 10000000UL;

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
  explicit AepMemoryController(uint64_t _max_size) {
    max_block_index_ = ceil((double)_max_size / BLOCK_LEN) + 1;
  }

  void Push(int _size, BLOCK_INDEX_TYPE _index){};

  bool Pop(int _size, BLOCK_INDEX_TYPE* _index) {
    *_index = current_block_index_.fetch_add(_size);
    return *_index < max_block_index_;
  }

 private:
  BLOCK_INDEX_TYPE max_block_index_;
  std::atomic<KEY_INDEX_TYPE> current_block_index_ = {0};
};

class KVStore {
 public:
  explicit KVStore(char* _memBase) : aep_base_(_memBase) {
    this->key_buffer_ = new char[KV_NUM_MAX * KEY_LEN];
    this->next_ = new KEY_INDEX_TYPE[KV_NUM_MAX];
    this->block_index_ = new BLOCK_INDEX_TYPE[KV_NUM_MAX];
    this->val_lens_ = new VALUE_LEN_TYPE[KV_NUM_MAX];
    this->versions_ = new VERSION_TYPE[KV_NUM_MAX]{0};
    this->aep_memory_controller_ = new AepMemoryController(FILE_SIZE);
    // TODO: ADD FREE LSIT
  };
  ~KVStore() {
    // delete this->freeList;
    delete this->next_;
    delete this->key_buffer_;
    delete this->block_index_;
    delete this->versions_;
    delete this->aep_memory_controller_;
  }

  // Read key and value according to the index of key
  void Read(KEY_INDEX_TYPE _index, string* _value) {
    BLOCK_INDEX_TYPE block_index = block_index_[_index];
    _value->assign(
        this->aep_base_ + (uint64_t)block_index * BLOCK_LEN + VALUE_OFFSET,
        val_lens_[_index]);
  }

  // Write kv pair to pmem
  void Write(const Slice& _key, const Slice& _value, Entry* _entry);

  void Update(const Slice& _key, const Slice& _value, KEY_INDEX_TYPE _index);

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

  BLOCK_INDEX_TYPE GetBlockIndex(const Slice& _value);

  void Recovery(BLOCK_INDEX_TYPE _block_index, VALUE_LEN_TYPE _value_len,
                char* _record, Entry* _entry) {
    KEY_INDEX_TYPE index;
    index = current_key_index_.fetch_add(1);
    auto oldHead = _entry->SetHead(index);
    next_[index] = oldHead;
    block_index_[index] = _block_index;
    val_lens_[index] = _value_len;
    versions_[index] = *(VERSION_TYPE*)(_record + VERSION_OFFSET);
    memcpy(key_buffer_ + index * KEY_LEN, _record + VAL_SIZE_LEN, KEY_LEN);
  }

  void UpdateKeyInfo(KEY_INDEX_TYPE _index, BLOCK_INDEX_TYPE _block_index,
                     VALUE_LEN_TYPE _value_len, VERSION_TYPE _version) {
    if (_version > versions_[_index]) {
      block_index_[_index] = _block_index;
      val_lens_[_index] = _value_len;
      versions_[_index] = _version;
    }
  }

 private:
  std::atomic<KEY_INDEX_TYPE> current_key_index_ = {0};
  KEY_INDEX_TYPE* next_ = nullptr;
  BLOCK_INDEX_TYPE* block_index_ = nullptr;
  VALUE_LEN_TYPE* val_lens_ = nullptr;
  VERSION_TYPE* versions_;
  char* key_buffer_ = nullptr;
  char* aep_base_ = nullptr;
  AepMemoryController* aep_memory_controller_ = nullptr;
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