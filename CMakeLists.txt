cmake_minimum_required(VERSION 3.12)
project(ext_list_ranking)

set(CMAKE_CXX_STANDARD 11)

add_executable(ext_list_ranking main.cpp)
add_executable(test_gen test_gen.cpp)

add_compile_definitions(DEFAULT_MEMORY_SIZE=512)
add_compile_options(-O2 -static -Wall -Wextra -x c++ --std=c++11)