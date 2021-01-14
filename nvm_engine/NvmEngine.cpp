#include "NvmEngine.hpp"
#include <sys/resource.h>
#include <sys/types.h>
#include <unistd.h>

FILE* NvmEngine::LOG;
typedef hash_func pFunction;
Status DB::CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file) {
  return NvmEngine::CreateOrOpen(name, dbptr, log_file);
}

DB::~DB() = default;

// TODO: Update Get block index
BLOCK_INDEX_TYPE KVStore::GetBlockIndex(const Slice& _value) {
  VALUE_LEN_TYPE dataLen = _value.size();
  int blockNum = (RECORD_FIX_LEN + dataLen + BLOCK_LEN - 1) / BLOCK_LEN;
  BLOCK_INDEX_TYPE bi = UINT32_MAX;
  // TODO: add Get bi from freelist and temp Set bi = 0
  bi = 0;
  return bi;
}

HashMap::HashMap(char* _base, pFunction _hash) : hash_(_hash) {
  std::allocator<Entry> entry_allocator;
  this->entries_ = entry_allocator.allocate(HASH_MAP_SIZE);
  for (size_t i = 0; i < HASH_MAP_SIZE; ++i) {
    entry_allocator.construct(this->entries_ + i);
  }
  kv_store_ = new KVStore(_base);
  Recovery(_base);
}

HashMap::~HashMap() {
  std::allocator<Entry> entry_allocator;
  entry_allocator.deallocate(this->entries_, HASH_MAP_SIZE);
  delete kv_store_;
}

Status HashMap::Get(const Slice& _key, std::string* _value) {
  uint32_t hashVal = DJBHash(_key.data());
  Entry& entry = this->entry(hashVal);
  KEY_INDEX_TYPE head = entry.GetHead();
  if (head == UINT32_MAX) return NotFound;
  head = kv_store_->Find(_key, head);
  if (head != UINT32_MAX) {
    kv_store_->read(head, _value);
    return Ok;
  }
  return NotFound;
}

Status HashMap::Set(const Slice& _key, const Slice& _value) {
  uint32_t hashVal = DJBHash(_key.data());
  Entry& entry = this->entry(hashVal);
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

Status HashMap::Recovery(char* _base) { return Ok; }

void HashMap::Summary() {
  struct rusage usage {};
  getrusage(RUSAGE_SELF, &usage);
  /*fprintf(NvmEngine::LOG,
          "Set %d(%d) timestamp:%ld Find times:%d rss:%ld cur block: %u\n", wt,
          000, time(nullptr), find_times, usage.ru_maxrss,
          this->kvStore->getValueIndex());*/
  fflush(NvmEngine::LOG);
}

Status NvmEngine::CreateOrOpen(const std::string& _name, DB** _dbptr,
                               FILE* _log_file) {
  auto* db = new NvmEngine(_name, _log_file);
  *_dbptr = db;
  return Ok;
}

NvmEngine::NvmEngine(const std::string& _name, FILE* _log_file) {
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

NvmEngine::~NvmEngine() { delete this->hash_map_; }

Status NvmEngine::Get(const Slice& key, std::string* value) {
  return hash_map_->Get(key, value);
}

Status NvmEngine::Set(const Slice& key, const Slice& value) {
  return hash_map_->Set(key, value);
}