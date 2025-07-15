add_library(util_io INTERFACE)
target_sources(util_io INTERFACE buffered_file_input.h buffered_file_output.h common.h)
target_link_libraries(util_io INTERFACE plog)
