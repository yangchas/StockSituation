cmake_minimum_required(VERSION 3.12)
project(HighPerformanceConsumer)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找依赖库
find_package(PkgConfig REQUIRED)

pkg_check_modules(rabbitmq REQUIRED librabbitmq)
pkg_check_modules(hiredis REQUIRED hiredis)
pkg_check_modules(zlib REQUIRED zlib)

# 假设TDengine库路径
set(TDENGINE_INCLUDE_DIRS "/usr/include/taos")
set(TDENGINE_LIBRARIES "taos")

# 包含目录
include_directories(
    ${rabbitmq_INCLUDE_DIRS}
    ${hiredis_INCLUDE_DIRS}
    ${zlib_INCLUDE_DIRS}
    ${TDENGINE_INCLUDE_DIRS}
)

# 可执行文件
add_executable(consumer 
    consumer.cpp
)

# 链接库
target_link_libraries(consumer
    ${rabbitmq_LIBRARIES}
    ${hiredis_LIBRARIES}
    ${zlib_LIBRARIES}
    ${TDENGINE_LIBRARIES}
    pthread
    dl
)

# 编译选项
target_compile_options(consumer PRIVATE
    -O3
    -march=native
    -Wall
    -Wextra
)