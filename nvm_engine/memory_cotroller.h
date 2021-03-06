//
// Created by andyshen on 1/15/21.
//
#pragma once
#include <mutex>
#include <stack>
#include <thread>
#include <unordered_map>
#include "define.h"

using std::stack;
using std::unordered_map;

std::mutex mt;
class FreeList {
 public:
  FreeList() = default;
  virtual ~FreeList() = default;
  virtual void Push(BLOCK_INDEX_TYPE _block_index, size_t _size) = 0;
  virtual bool Pop(BLOCK_INDEX_TYPE* _block_index, size_t _size) = 0;
  bool ThreadSafePop(BLOCK_INDEX_TYPE* _block_index, size_t _size) {
    bool flag;
    mt.lock();
    flag = Pop(_block_index, _size);
    mt.unlock();
    return flag;
  }
  virtual void MergeTo(FreeList* src_free_list, FreeList* dst_free_list) = 0;
};

class SimpleFreeList : public FreeList {
 public:
  void Push(BLOCK_INDEX_TYPE _block_index, size_t _size) override {
    auto iter = map_.find(_size);
    if (iter == map_.cend()) {
      std::vector<BLOCK_INDEX_TYPE> temp;
      temp.emplace_back(_block_index);
    } else {
      iter->second.push(_block_index);
    }
  }

  bool Pop(BLOCK_INDEX_TYPE* _block_index, size_t _size) override {
    auto iter = map_.find(_size);
    if (iter == map_.cend()) {
      return false;
    } else {
      if (iter->second.empty()) {
        return false;
      }
      *_block_index = iter->second.top();
      iter->second.pop();
    }
    return true;
  }

  void MergeTo(FreeList* src_free_list, FreeList* dst_free_list) override {}

 private:
  unordered_map<uint8_t, stack<BLOCK_INDEX_TYPE>> map_;
};

class GlobalMemoryController {
 public:
  explicit GlobalMemoryController(size_t _file_size) {
    max_segment_index_ =
        _file_size / (CONFIG.block_per_segment_ * CONFIG.block_size_);
    global_free_list_ = new SimpleFreeList();
  }
  ~GlobalMemoryController() { delete global_free_list_; }
  bool Allocate(BLOCK_INDEX_TYPE* _block_index) {
    SEGMENT_INDEX_TYPE segment_index = this->segment_index_.fetch_add(1);
    if (segment_index >= max_segment_index_) {
      if (!free_segments.empty()) {
        *_block_index = free_segments.top() * CONFIG.block_per_segment_;
        free_segments.pop();
        return true;
      }
      return false;
    }
    *_block_index = segment_index * CONFIG.block_per_segment_;
    /*std::cout << "Thread id:" << std::this_thread::get_id()
              << " allocate an segment, block index:" << *_block_index
              << std::endl;*/
    return true;
  }

  bool New(BLOCK_INDEX_TYPE* _block_index, size_t _buffer_size) {
    size_t num_segments =
        (_buffer_size + CONFIG.block_per_segment_ * CONFIG.block_size_ - 1) /
        (CONFIG.block_per_segment_ * CONFIG.block_size_);
    SEGMENT_INDEX_TYPE segment_index = segment_index_.fetch_add(num_segments);
    if (segment_index >= max_segment_index_) {
      std::cout << "OOM: Failed to new a big array." << std::endl;
      return false;
    }
    *_block_index = segment_index * CONFIG.block_per_segment_;
    return true;
  }

  void Delete(BLOCK_INDEX_TYPE block_index_, size_t _buffer_size) {
    size_t num_segments =
        (_buffer_size + CONFIG.block_per_segment_ * CONFIG.block_size_ - 1) /
        (CONFIG.block_per_segment_ * CONFIG.block_size_);
    for (unsigned int i = 0; i < num_segments; i++) {
      free_segments.push(block_index_ + i * CONFIG.block_per_segment_);
    }
  }
  FreeList* free_list() const { return global_free_list_; }

 private:
  SEGMENT_INDEX_TYPE max_segment_index_;
  std::atomic<SEGMENT_INDEX_TYPE> segment_index_{0};
  FreeList* global_free_list_;
  std::stack<BLOCK_INDEX_TYPE> free_segments;
};

class AepMemoryController {
 public:
  static GlobalMemoryController* global_memory_;

 public:
  explicit AepMemoryController() {
    if (!global_memory_->Allocate(&current_block_index_)) {
      std::cout << "Out of memory when allocate segment." << std::endl;
      abort();
    }
    max_block_index_ = current_block_index_ + CONFIG.block_per_segment_;
    free_list_ = new SimpleFreeList();
  }
  ~AepMemoryController() { delete free_list_; }

  bool New(int _size, BLOCK_INDEX_TYPE* _index) {
    if (current_block_index_ + _size > max_block_index_) {
      if (free_list_->Pop(_index, _size)) {
        return true;
      }
      // recycle rest block.
      size_t size = max_block_index_ - current_block_index_;
      free_list_->Push(current_block_index_, size);
      if (global_memory_->Allocate(&current_block_index_)) {
        max_block_index_ = current_block_index_ + CONFIG.block_per_segment_;
        *_index = current_block_index_;
        current_block_index_ += _size;
        return true;
      } else {
        auto free_list = global_memory_->free_list();
        return free_list->ThreadSafePop(_index, _size);
      }
    } else {
      *_index = current_block_index_;
      current_block_index_ += _size;
      return true;
    }
  };

  bool Delete(int _size, BLOCK_INDEX_TYPE _index) {
    free_list_->Push(_index, _size);
    return true;
  }

 private:
  FreeList* free_list_;
  BLOCK_INDEX_TYPE max_block_index_;
  BLOCK_INDEX_TYPE current_block_index_;
};