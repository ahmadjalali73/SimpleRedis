cmake_minimum_required(VERSION 3.5)

project(Redis-client)

file(GLOB
    SOURCES
    src/*.cpp)

add_executable(redis-client ${SOURCES})

target_include_directories(redis-client
        PRIVATE
        ${PROJECT_SOURCE_DIR}/include)