# minimum version of cmake
cmake_minimum_required (VERSION 3.1)

# project name
project (spdist_exe)

add_subdirectory(common)
add_subdirectory(dist)
add_subdirectory(ec)
add_subdirectory(protocol)
add_subdirectory(util)

aux_source_directory(. DIR_SRCS)
add_executable(DistCoordinator DistCoordinator.cc)
add_executable(DistAgent DistAgent.cc)
add_executable(DistClient DistClient.cc)
add_executable(ClayTest ClayTest.cc)
add_executable(GenData GenData.cc)
add_executable(RepairTest RepairTest.cc)
add_executable(TradeoffAnalysis TradeoffAnalysis.cc)
add_executable(SimpleSearch SimpleSearch.cc)
add_executable(LocalSearch LocalSearch.cc)
#add_executable(ECDAGTest ECDAGTest.cc)
#add_executable(CodeTest CodeTest.cc)
#add_executable(LocalTest LocalTest.cc)

target_link_libraries(DistCoordinator common pthread protocol ec)
target_link_libraries(DistAgent common pthread ec)
target_link_libraries(DistClient common pthread protocol)
target_link_libraries(ClayTest common ec protocol)
target_link_libraries(GenData common ec protocol)
target_link_libraries(RepairTest common ec protocol)
target_link_libraries(TradeoffAnalysis common ec protocol)
target_link_libraries(SimpleSearch common ec protocol)
target_link_libraries(LocalSearch common ec protocol dist)
#target_link_libraries(ECDAGTest common ec)
#target_link_libraries(CodeTest common ec)
#target_link_libraries(LocalTest common ec)

