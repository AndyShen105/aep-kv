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

BLOCK_INDEX_TYPE KVStore::getBlockIndex(const Slice& _value) {
  VALUE_LEN_TYPE dataLen = _value.size();
  VALUE_LEN_TYPE blockNum = (dataLen + VALUE_BLOCK_LEN - 1) / VALUE_BLOCK_LEN;

  BLOCK_INDEX_TYPE bi = UINT32_MAX;
  bool found = this->freeList->pop(blockNum, &bi);
  if (!found) {
    bi = this->curValueIndex.fetch_add(blockNum);
  }
  return bi;
}

HashMap::HashMap(char* _base, pFunction _hash) : hash(_hash) {
  std::allocator<Entry> entry_allocator;
  this->entries = entry_allocator.allocate(HASH_MAP_SIZE);
  for (size_t i = 0; i < HASH_MAP_SIZE; ++i) {
    entry_allocator.construct(this->entries + i);
  }
  kvStore = new KVStore(_base);
  recovery(_base);
}

HashMap::~HashMap() {
  std::allocator<Entry> entry_allocator;
  entry_allocator.deallocate(this->entries, HASH_MAP_SIZE);
  delete kvStore;
}

Status HashMap::get(const Slice& _key, std::string* _value) {
  if (tid == -1) {
    tid = curTid.fetch_add(1);
    tid %= THREAD_NUM;
  }

  uint32_t hashVal = DJBHash(_key.data());
  Entry& entry = this->entry(hashVal);
  KEY_INDEX_TYPE head = entry.getHead();
  if (head == UINT32_MAX) return NotFound;
  head = kvStore->key_pool->find(_key, head);
  if (head != UINT32_MAX) {
    kvStore->read(head, _value);
    return Ok;
  }
  return NotFound;
}

Status HashMap::set(const Slice& _key, const Slice& _value) {
  if (tid == -1) {
    tid = curTid.fetch_add(1);
    tid %= THREAD_NUM;
    usleep(tid * 50);
  }

  uint32_t hashVal = DJBHash(_key.data());
  Entry& entry = this->entry(hashVal);
  KEY_INDEX_TYPE head = entry.getHead();
  if (head != UINT32_MAX) {
    head = kvStore->key_pool->find(_key, entry.getHead());
  }

  if (head == UINT32_MAX) {
    kvStore->write(_key, _value, &entry);
    return Ok;
  }

  kvStore->update(_key, _value, head);
  return Ok;
}

Status HashMap::recovery(char* _base) {
  KEY_INDEX_TYPE currentIndex = 0;
  BLOCK_INDEX_TYPE current_max_block = 0;
  // recovery key buffer.
  kvStore->key_pool->recoveryKeyBuffer();
  while (currentIndex < KV_NUM_MAX) {
    // TODO：FIX TYPE uint32 to uint64
    char* key = _base + (uint64_t)currentIndex * KEY_STORE_LEN;
    memcpy(keyBuff, key, KEY_STORE_LEN);
    VALUE_LEN_TYPE val_len = *(VALUE_LEN_TYPE*)(key + KEY_LEN);
    if (val_len == 0) {
      fprintf(NvmEngine::LOG, "recovery data size: %d  timestamp:%ld \n",
              currentIndex, time(nullptr));
      break;
    }
    // get max block index.
    BLOCK_INDEX_TYPE current_block =
        *(BLOCK_INDEX_TYPE*)(key + KEY_STORE_VALUE_LINK_OFFSET);

    current_max_block = std::max(current_max_block, current_block);
    // update entry
    uint32_t hashVal = this->hashValue(keyBuff);
    Entry& entry = this->entry(hashVal);
    KEY_INDEX_TYPE head = entry.getHead();
    entry.setHead(kvStore->key_pool->recovery(head));
    currentIndex++;
  }
  fprintf(NvmEngine::LOG, "recovery block index: %d  timestamp:%ld \n",
          current_max_block, time(nullptr));

  kvStore->setValueIndex(current_max_block + 32 +
                         VALUE_PART_FIX_NUM * THREAD_NUM);

  auto oldKeyIndex =
      kvStore->key_pool->currentIndex.fetch_add(KEY_PART_FIX_NUM * THREAD_NUM);

  fprintf(NvmEngine::LOG, "old key index: %d\n", oldKeyIndex);
  fflush(NvmEngine::LOG);

  kvStore->key_pool->setLocalKey(oldKeyIndex);
  kvStore->freeList->setLocalFree(current_max_block + 32);
  return Ok;
}

void HashMap::summary() {
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  fprintf(NvmEngine::LOG,
          "set %d(%d) timestamp:%ld find times:%d rss:%ld cur block: %u\n", wt,
          tid, time(NULL), find_times, usage.ru_maxrss,
          this->kvStore->getValueIndex());
  fflush(NvmEngine::LOG);
}

Status NvmEngine::CreateOrOpen(const std::string& name, DB** dbptr,
                               FILE* log_file) {
  auto* db = new NvmEngine(name, log_file);
  *dbptr = db;
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
  hashMap = new HashMap(base);
}

NvmEngine::~NvmEngine() { delete this->hashMap; }

Status NvmEngine::Get(const Slice& key, std::string* value) {
  /*++rt;
  if (rt == 15000000) {
    hashMap->summary();
    exit(0);
  }*/
  return hashMap->get(key, value);
}

Status NvmEngine::Set(const Slice& key, const Slice& value) {
  /* ++wt;

   if (wt == 25165823) {
     hashMap->summary();
     *//*fprintf(NvmEngine::LOG, "%s",
            this->hashMap->kvStore->freeList->getStatus().c_str());
    fflush(NvmEngine::LOG);*//*
    exit(0);
  }

  if (wt % 5000000 == 0) {
    this->hashMap->summary();
  }*/

  return hashMap->set(key, value);
}