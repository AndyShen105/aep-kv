//
// Created by andyshen on 1/15/21.
//
#pragma once
typedef uint32_t HASH_VALUE;
typedef uint16_t VALUE_LEN_TYPE;
typedef uint32_t KEY_INDEX_TYPE;
typedef uint32_t BLOCK_INDEX_TYPE;
typedef uint16_t VERSION_TYPE;
typedef uint32_t SEGMENT_INDEX_TYPE;

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
