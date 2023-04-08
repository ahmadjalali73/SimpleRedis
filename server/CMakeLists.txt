cmake_minimum_required(VERSION 3.5)

project(Redis)

file(GLOB
    SOURCES
    src/*.cpp)

add_executable(redis ${SOURCES})
SET(CMAKE_CXX_FLAGS_DEBUG "-g")
SET(CMAKE_CXX_FLAGS_RELEASE "-O3")
# add_compile_options(redis PRIVATE -O3 -g3)

set(CMAKE_CXX_FLAGS_DEBUG_INIT "-Wall")
set(CMAKE_CXX_FLAGS_RELEASE_INIT "-Wall")

target_include_directories(redis
        PRIVATE
        ${PROJECT_SOURCE_DIR}/include)