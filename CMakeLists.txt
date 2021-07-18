cmake_minimum_required(VERSION 3.17)
project(lsm-kv)

set(CMAKE_CXX_STANDARD 14)

add_executable(lsm-kv
        persistence.cc
        kvstore.cc
        bloomfilter.cc
        skiplist.cc

        )