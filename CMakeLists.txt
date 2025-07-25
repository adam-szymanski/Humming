cmake_minimum_required(VERSION 3.22)
project(KV CXX)

set(CMAKE_CXX_STANDARD 20)

add_custom_target(
        format_code
        COMMAND cd ${CMAKE_SOURCE_DIR} && bash format_changes.sh
        COMMENT "Formatting changes...")

include(FetchContent)
include(ExternalProject)

FetchContent_Declare(
        plog
        GIT_REPOSITORY https://github.com/SergiusTheBest/plog.git
        GIT_TAG "1.1.10"
        GIT_SHALLOW TRUE
        GIT_PROGRESS ON
        SYSTEM)
FetchContent_MakeAvailable(plog)

include_directories(${CMAKE_SOURCE_DIR})
add_subdirectory(util)
add_subdirectory(db)

add_library(humming_lib INTERFACE)
target_sources(humming_lib INTERFACE dummy.h)

target_link_libraries(
        humming_lib
        INTERFACE -lpthread
        plog
        util_perf
        util_io
        db)

add_executable(humming main.cpp)
target_link_libraries(humming humming_lib)

