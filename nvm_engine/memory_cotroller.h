//
// Created by andyshen on 1/15/21.
//
#pragma once
#include "define.h"

class FreeList {
 public:
  // TODO: 分配一个_size个Block的空间
  void Push(BLOCK_INDEX_TYPE* _block_index, size_t _size);
  // TODO: 将一个block index 为_block_index且size为_size
  // 个block的空间存入freelist
  void Pop(BLOCK_INDEX_TYPE _block_index, size_t _size);

  void MergeTo(FreeList* src_free_list, FreeList* dis_free_list);
 private:
  // XXX
};

class GlobalMemory {
 public:
  static const size_t kBlockPerSeg = 65536;

 public:
  explicit GlobalMemory(size_t _file_size) {
    max_segment_index_ = _file_size / kBlockPerSeg;
  }
  bool Allocate(BLOCK_INDEX_TYPE* _block_index) {
    SEGMENT_INDEX_TYPE segment_index = this->segment_index_.fetch_add(1);
    if (segment_index >= max_segment_index_) {
      return false;
    }
    *_block_index = segment_index * kBlockPerSeg;
    return true;
  }

 private:
  SEGMENT_INDEX_TYPE max_segment_index_;
  std::atomic<SEGMENT_INDEX_TYPE> segment_index_{0};
};

class AepMemoryController {
 public:
  static GlobalMemory* global_memory_;

 public:
  explicit AepMemoryController() {
    max_block_index_ = GlobalMemory::kBlockPerSeg;
    if (!global_memory_->Allocate(&current_block_index_)) {
      std::cout << "Out of memory" << std::endl;
      exit(2);
    }
  }

  bool New(int _size, BLOCK_INDEX_TYPE* _index) {
    if (current_block_index_ + _size > max_block_index_) {
      // TODO: free list recycle
      if (!global_memory_->Allocate(&current_block_index_)) {
        max_block_index_ += current_block_index_ + GlobalMemory::kBlockPerSeg;
        *_index = current_block_index_++;
        return true;
      } else {
        // TODO:: if can not allocate, malloc from global free list.
        exit(2);
      }
    } else {
      *_index = current_block_index_++;
      return true;
    }
  };

  bool Delete(int _size, BLOCK_INDEX_TYPE* _index) { return true; }

 private:
  BLOCK_INDEX_TYPE max_block_index_;
  BLOCK_INDEX_TYPE current_block_index_;
};