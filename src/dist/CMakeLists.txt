aux_source_directory(. DIR_LIB_SRCS)
add_library (dist ${DIR_LIB_SRCS})
target_link_libraries(dist util)

