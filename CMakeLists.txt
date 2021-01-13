cmake_minimum_required(VERSION 3.16)
project(tair_contest)

set(CMAKE_CXX_STANDARD 14)

include_directories(include)
include_directories(nvm_engine)
LINK_DIRECTORIES(/usr/local/lib)
add_executable(tair_contest
        include/db.hpp
        nvm_engine/NvmEngine.cpp
        nvm_engine/NvmEngine.hpp
        nvm_engine/concurrentqueue.h
        test/test.cpp)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pg")
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pg")
SET(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -pg")
target_link_libraries(tair_contest -lpmem)