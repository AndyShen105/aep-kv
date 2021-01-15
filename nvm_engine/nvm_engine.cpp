#include "nvm_engine.hpp"
#include <sys/resource.h>
#include <sys/types.h>
#include <unistd.h>

FILE* nvm_engine::LOG;
typedef hash_func pFunction;

GlobalMemory* AepMemoryController::global_memory_ = new GlobalMemory(FILE_SIZE);

Status DB::CreateOrOpen(const std::string& _name, DB** _db, FILE* _log_file) {
  return nvm_engine::CreateOrOpen(_name, _db, _log_file);
}

DB::~DB() = default;

BLOCK_INDEX_TYPE KVStore::GetBlockIndex(const Slice& _value) {
  VALUE_LEN_TYPE data_len = _value.size();
  int block_num = (RECORD_FIX_LEN + data_len + BLOCK_LEN - 1) / BLOCK_LEN;
  BLOCK_INDEX_TYPE block_index = UINT32_MAX;
  if (!thread_local_aep_controller->New(block_num, &block_index)) {
    block_index = UINT32_MAX;
    std::cout << "Out of memory, when allocate an aep space." << std::endl;
    abort();
  }
  return block_index;
}

void KVStore::Write(const Slice& _key, const Slice& _value, Entry* _entry) {

  BLOCK_INDEX_TYPE bi = 0;//GetBlockIndex(_value);
  size_t record_len = RECORD_FIX_LEN + _value.size();
  char record_buffer[record_len];
  VALUE_LEN_TYPE len = _value.size();
  VERSION_TYPE version = 0;
  memcpy(record_buffer + KEY_OFFSET, _key.data(), KEY_LEN);
  memcpy(record_buffer + VAL_SIZE_OFFSET, &len, VAL_SIZE_LEN);
  memcpy(record_buffer + VERSION_OFFSET, &version, VERSION_LEN);
  memcpy(record_buffer + VALUE_OFFSET, _value.data(), _value.size());
  HASH_VALUE check_sum = DJBHash(record_buffer, record_len - CHECK_SUM_LEN);
  memcpy(record_buffer + record_len - CHECK_SUM_LEN, &check_sum, CHECK_SUM_LEN);
  // memcpy to pmem and flush

  pmem_memcpy_nodrain(this->aep_base_ + (uint64_t)bi * BLOCK_LEN, record_buffer,
                      record_len);

  // Update key buffer in memory
  KEY_INDEX_TYPE index;
  index = current_key_index_.fetch_add(1);
  auto oldHead = _entry->SetHead(index);
  next_[index] = oldHead;
  block_index_[index] = bi;
  val_lens_[index] = _value.size();
  memcpy(key_buffer_ + index * KEY_LEN, _key.data(), KEY_LEN);
}

void KVStore::Update(const Slice& _key, const Slice& _value,
                     KEY_INDEX_TYPE _index) {
  VALUE_LEN_TYPE data_len = val_lens_[_index];
  BLOCK_INDEX_TYPE old_block_index = block_index_[_index];

  BLOCK_INDEX_TYPE new_block_index = GetBlockIndex(_value);
  size_t record_len = RECORD_FIX_LEN + _value.size();
  char record_buffer[record_len];
  VALUE_LEN_TYPE len = _value.size();
  VERSION_TYPE version = versions_[_index] + 1;
  memcpy(record_buffer + KEY_OFFSET, _key.data(), KEY_LEN);
  memcpy(record_buffer + VAL_SIZE_OFFSET, &len, VAL_SIZE_LEN);
  memcpy(record_buffer + VERSION_OFFSET, &version, VERSION_LEN);
  memcpy(record_buffer + VALUE_OFFSET, _value.data(), _value.size());
  HASH_VALUE check_sum = DJBHash(record_buffer, record_len - CHECK_SUM_LEN);
  memcpy(record_buffer + record_len - CHECK_SUM_LEN, &check_sum, CHECK_SUM_LEN);
  // memcpy to pmem and flush
  pmem_memcpy_persist(this->aep_base_ + (uint64_t)new_block_index * BLOCK_LEN,
                      record_buffer, record_len);
  block_index_[_index] = new_block_index;
  versions_[_index] = version;
  val_lens_[_index] = _value.size();
  Recycle(data_len, old_block_index);
}

HashMap::HashMap(char* _base, pFunction _hash) : hash_(_hash) {
  std::allocator<Entry> entry_allocator;
  this->entries_ = entry_allocator.allocate(HASH_MAP_SIZE);
  for (size_t i = 0; i < HASH_MAP_SIZE; ++i) {
    entry_allocator.construct(this->entries_ + i);
  }
  kv_store_ = new KVStore(_base);
  // Recovery(_base);
}

HashMap::~HashMap() {
  std::allocator<Entry> entry_allocator;
  entry_allocator.deallocate(this->entries_, HASH_MAP_SIZE);
  delete kv_store_;
}

Status HashMap::Get(const Slice& _key, std::string* _value) {
  uint32_t hash_val = DJBHash(_key.data());
  Entry& entry = this->entry(hash_val);
  KEY_INDEX_TYPE head = entry.GetHead();
  if (head == UINT32_MAX) return NotFound;
  head = kv_store_->Find(_key, head);
  if (head != UINT32_MAX) {
    kv_store_->Read(head, _value);
    return Ok;
  }
  return NotFound;
}

Status HashMap::Set(const Slice& _key, const Slice& _value) {

  uint32_t hash_val = DJBHash(_key.data());
  Entry& entry = this->entry(hash_val);
  KEY_INDEX_TYPE head = entry.GetHead();
  if (head != UINT32_MAX) {
    head = kv_store_->Find(_key, entry.GetHead());
  }

  if (head == UINT32_MAX) {
    kv_store_->Write(_key, _value, &entry);
    return Ok;
  }

  kv_store_->Update(_key, _value, head);
  return Ok;
}

Status HashMap::Recovery(char* _base) {
  size_t offset = 0;
  char* record_base = nullptr;
  VALUE_LEN_TYPE len = UINT16_MAX;
  HASH_VALUE check_sum = UINT32_MAX;
  uint16_t record_len = 0;

  while (offset < FILE_SIZE / BLOCK_LEN) {
    // check max offset
    record_base = _base + (uint64_t)offset * BLOCK_LEN;
    len = *(VALUE_LEN_TYPE*)(record_base);
    record_len = len + RECORD_FIX_LEN;
    HASH_VALUE check_sum_new = DJBHash(record_base, record_len - CHECK_SUM_LEN);
    check_sum = *(HASH_VALUE*)(record_base + (record_len - CHECK_SUM_LEN));
    if (check_sum_new == check_sum) {
      uint32_t hash_val = DJBHash(record_base + VAL_SIZE_LEN);
      Entry& entry = this->entry(hash_val);
      KEY_INDEX_TYPE head = entry.GetHead();
      if (head != UINT32_MAX) {
        head = kv_store_->Find(record_base + VAL_SIZE_LEN, entry.GetHead());
      }
      if (head == UINT32_MAX) {
        this->kv_store_->Recovery(offset, len, record_base, &entry);
      } else {
        this->kv_store_->UpdateKeyInfo(
            head, offset, len, *(VERSION_TYPE*)(record_base + VERSION_OFFSET));
      }
      int blockNum = (record_len + BLOCK_LEN - 1) / BLOCK_LEN;
      offset += blockNum;
    } else {
      // recycle
      offset++;
    }
  }
  return Ok;
}

void HashMap::Summary() {
  struct rusage usage {};
  getrusage(RUSAGE_SELF, &usage);
  /*fprintf(NvmEngine::LOG,
          "Set %d(%d) timestamp:%ld Find times:%d rss:%ld cur block: %u\n", wt,
          000, time(nullptr), find_times, usage.ru_maxrss,
          this->kvStore->getValueIndex());*/
  fflush(nvm_engine::LOG);
}

Status nvm_engine::CreateOrOpen(const std::string& _name, DB** _dbptr,
                                FILE* _log_file) {
  auto* db = new nvm_engine(_name, _log_file);
  *_dbptr = db;
  return Ok;
}

nvm_engine::nvm_engine(const std::string& _name, FILE* _log_file) {
  LOG = _log_file;
  char* base;
  int isPmem;
  if ((base = (char*)pmem_map_file(_name.c_str(), FILE_SIZE, PMEM_FILE_CREATE,
                                   0666, 0, &isPmem)) == nullptr) {
    perror("Pmem map file failed");
    exit(1);
  }
  hash_map_ = new HashMap(base);
}

nvm_engine::~nvm_engine() { delete this->hash_map_; }

Status nvm_engine::Get(const Slice& key, std::string* value) {
  return hash_map_->Get(key, value);
}

Status nvm_engine::Set(const Slice& key, const Slice& value) {
  /*if (write_count_++ % 5000 == 0) {
    std::cout << "write requests:" << write_count_ << std::endl;
  }*/
  return hash_map_->Set(key, value);
}