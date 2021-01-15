#!/bin/bash

INCLUDE_DIR="../include"
LIB_PATH="../lib"
g++ -pthread -o judge judge.cpp random.cpp\
	-L $LIB_PATH -l engine \
	-I $INCLUDE_DIR \
	  -lpmem \
    -g \
    -mavx2 \
    -std=c++11

if [ $? -ne 0 ]; then
    echo "Compile Error"
    exit 7 
fi

