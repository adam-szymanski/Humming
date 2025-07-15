add_library(db INTERFACE)
target_sources(db INTERFACE bucket.h bucket.cpp KV.h KV.cpp)
target_link_libraries(db INTERFACE plog util_io util_perf)
