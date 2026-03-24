cmake_minimum_required(VERSION 3.10 FATAL_ERROR)

project(arkilian C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

# Enable strict warnings
if(CMAKE_C_COMPILER_ID MATCHES "MSVC")
    add_compile_options(/W4 /WX)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else()
    add_compile_options(-Wall -Wextra -Wpedantic -Werror)
endif()

# Find CURL
find_package(CURL REQUIRED)

# Find Threads
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(arkilian 
    src/main.c 
    src/class.c 
    src/deps/sqlite/sqlite3.c
)

target_include_directories(arkilian PRIVATE src src/deps/sqlite)

target_link_libraries(arkilian PRIVATE CURL::libcurl Threads::Threads)
