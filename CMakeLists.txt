# minimum version of cmake
cmake_minimum_required (VERSION 3.1)

# project name
project (ParaRC)

set (CMAKE_CXX_STANDARD 11)
set (CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
#set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -mmmx -msse -mpclmul -msse4.2 -DINTEL_SSE4 -mavx")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -mmmx -msse -mpclmul -msse4.2 -DINTEL_SSE4")

# subdirectory
add_subdirectory(src)
