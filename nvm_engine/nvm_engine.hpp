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
#include "define.h"
#include "memory_cotroller.h"

using std::atomic;
using std::string;
using std::vector;

GlobalMemoryController* AepMemoryController::global_memory_ = new GlobalMemoryController(FILE_SIZE);
thread_local AepMemoryController* thread_local_aep_controller =
    new AepMemoryController;

thread_local size_t write_count_{0};

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

class KVStore {
 public:
  explicit KVStore(char* _memBase, bool is_allocate_aep = false)
      : aep_base_(_memBase) {
    is_allocate_aep_ = is_allocate_aep;
    if (is_allocate_aep_) {
      BLOCK_INDEX_TYPE block_index;
      if (AepMemoryController::global_memory_->New(&block_index,
                                                   KV_NUM_MAX * KEY_LEN)) {
        this->key_buffer_ =
            _memBase + (uint64_t)block_index * CONFIG.block_size_;
      } else {
        std::cout << "reallocate memory from memory." << std::endl;
        this->key_buffer_ = new char[KV_NUM_MAX * KEY_LEN];
      }
    } else {
      this->key_buffer_ = new char[KV_NUM_MAX * KEY_LEN];
    }

    this->next_ = new KEY_INDEX_TYPE[KV_NUM_MAX];
    this->block_index_ = new BLOCK_INDEX_TYPE[KV_NUM_MAX];
    this->val_lens_ = new VALUE_LEN_TYPE[KV_NUM_MAX];
    this->versions_ = new VERSION_TYPE[KV_NUM_MAX]{0};
    // TODO: ADD FREE LSIT
  };
  ~KVStore() {
    // delete this->freeList;
    delete this->next_;
    delete this->block_index_;
    delete this->versions_;
    if (is_allocate_aep_) {
      BLOCK_INDEX_TYPE block_index =
          (this->key_buffer_ - this->aep_base_) / CONFIG.block_size_;
      AepMemoryController::global_memory_->Delete(block_index,
                                                  KV_NUM_MAX * KEY_LEN);
    } else {
      delete this->key_buffer_;
    }
  }

  // Read key and value according to the index of key
  void Read(KEY_INDEX_TYPE _index, string* _value) {
    BLOCK_INDEX_TYPE block_index = block_index_[_index];
    _value->assign(this->aep_base_ +
                       (uint64_t)block_index * CONFIG.block_size_ +
                       VALUE_OFFSET,
                   val_lens_[_index]);
  }

  // Write kv pair to pmem
  void Write(const Slice& _key, const Slice& _value, Entry* _entry);

  void Update(const Slice& _key, const Slice& _value, KEY_INDEX_TYPE _index);

  // Recycle value according to its head index
  void Recycle(VALUE_LEN_TYPE _dataLen, BLOCK_INDEX_TYPE _index) {
    // TODO: ADD freelist
    int size = (RECORD_FIX_LEN + _dataLen + CONFIG.block_size_ - 1) /
               CONFIG.block_size_;
    thread_local_aep_controller->Delete(size, _index);
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
  bool is_allocate_aep_;
  std::atomic<KEY_INDEX_TYPE> current_key_index_ = {0};
  KEY_INDEX_TYPE* next_ = nullptr;
  BLOCK_INDEX_TYPE* block_index_ = nullptr;
  VALUE_LEN_TYPE* val_lens_ = nullptr;
  VERSION_TYPE* versions_;
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
  Entry& entry(const uint32_t _hash) { return entries_[_hash % HASH_MAP_SIZE]; }

 private:
  Entry* entries_;
  hash_func hash_;
};

class NvmEngine : DB {
 public:
  static FILE* LOG;

 public:
  static Status CreateOrOpen(const std::string& _name, Config* _config,
                             DB** _dbptr, FILE* _log_file = nullptr);

  NvmEngine(const std::string& _name, FILE* _log_file);

  ~NvmEngine() override;

  Status Get(const Slice& _key, std::string* _value) override;

  Status Set(const Slice& _key, const Slice& _value) override;

 private:
  HashMap* hash_map_;
};