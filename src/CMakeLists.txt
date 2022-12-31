# minimum version of cmake
cmake_minimum_required (VERSION 3.1)

# project name
project (ParaRC_exe)

add_subdirectory(common)
add_subdirectory(dist)
add_subdirectory(ec)
add_subdirectory(protocol)
add_subdirectory(util)

aux_source_directory(. DIR_SRCS)
add_executable(DistCoordinator DistCoordinator.cc)
add_executable(DistAgent DistAgent.cc)
add_executable(DistClient DistClient.cc)
add_executable(BruteForce BruteForce.cc)
add_executable(GenMLP GenMLP.cc)
add_executable(CodeTest CodeTest.cc)
add_executable(GenData GenData.cc)

target_link_libraries(DistCoordinator common pthread protocol ec)
target_link_libraries(DistAgent common pthread ec)
target_link_libraries(DistClient common pthread protocol)
target_link_libraries(BruteForce common ec protocol)
target_link_libraries(GenMLP common ec protocol dist)
target_link_libraries(GenData common ec protocol dist)
target_link_libraries(CodeTest common ec)

