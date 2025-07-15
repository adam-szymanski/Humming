add_library(util_perf INTERFACE)
target_sources(util_perf INTERFACE timer.h timer.cpp)
target_link_libraries(util_perf INTERFACE plog)
