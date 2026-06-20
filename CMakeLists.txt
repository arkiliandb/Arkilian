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

# Options for library types
option(ARKILIAN_BUILD_SHARED "Build shared library for FFI (Node/Python)" ON)
option(ARKILIAN_BUILD_STATIC "Build static library for embedded use" ON)
option(ARKILIAN_BUILD_EXAMPLES "Build example programs" ON)
option(ARKILIAN_BUILD_TESTS "Build test programs" OFF)

# Find CURL
find_package(CURL REQUIRED)

# Find Threads
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Common source files
set(ARKILIAN_SOURCES src/class.c src/deps/sqlite/sqlite3.c)

# Shared Library
if(ARKILIAN_BUILD_SHARED)
    add_library(arkilian SHARED ${ARKILIAN_SOURCES})
    target_include_directories(arkilian PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/deps/sqlite>
        $<INSTALL_INTERFACE:include/arkilian>
    )
    target_link_libraries(arkilian PRIVATE CURL::libcurl Threads::Threads)
    set_target_properties(arkilian PROPERTIES
        VERSION 1.0.0
        SOVERSION 1
        PUBLIC_HEADER "src/class.h;src/deps/sqlite/sqlite3.h"
    )
endif()

# Static Library
if(ARKILIAN_BUILD_STATIC)
    add_library(arkilian_static STATIC ${ARKILIAN_SOURCES})
    target_include_directories(arkilian_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/deps/sqlite>
        $<INSTALL_INTERFACE:include/arkilian>
    )
    target_link_libraries(arkilian_static PRIVATE CURL::libcurl Threads::Threads)
    set_target_properties(arkilian_static PROPERTIES
        OUTPUT_NAME arkilian
        PUBLIC_HEADER "src/class.h;src/deps/sqlite/sqlite3.h"
    )
endif()

# Examples
if(ARKILIAN_BUILD_EXAMPLES)
    add_executable(arkilian_example examples/main.c)
    if(ARKILIAN_BUILD_SHARED)
        target_link_libraries(arkilian_example PRIVATE arkilian)
    else()
        target_link_libraries(arkilian_example PRIVATE arkilian_static)
    endif()
endif()

# Node.js N-API Addon (built via cmake-js)
option(ARKILIAN_BUILD_NAPI "Build Node.js N-API addon" OFF)

if(CMAKE_JS_VERSION OR ARKILIAN_BUILD_NAPI)
    # cmake-js helper sources (needed for Windows delay-load hook)
    if(CMAKE_JS_SRC)
        file(GLOB CMAKE_JS_SRC_FILES "${CMAKE_JS_SRC}/*.cc" "${CMAKE_JS_SRC}/*.c")
    else()
        set(CMAKE_JS_SRC_FILES "")
    endif()

    add_library(arkilian_napi SHARED
        bindings/node/addon.c
        ${ARKILIAN_SOURCES}
        ${CMAKE_JS_SRC_FILES}
    )

    set_target_properties(arkilian_napi PROPERTIES
        PREFIX ""
        SUFFIX ".node"
        OUTPUT_NAME "arkilian"
    )

    target_include_directories(arkilian_napi PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/src/deps/sqlite
    )

    # Node.js headers (provided by cmake-js)
    if(CMAKE_JS_INC)
        target_include_directories(arkilian_napi PRIVATE ${CMAKE_JS_INC})
    endif()

    target_link_libraries(arkilian_napi PRIVATE CURL::libcurl Threads::Threads)

    # Node.js import library (needed on Windows)
    if(CMAKE_JS_LIB)
        target_link_libraries(arkilian_napi PRIVATE ${CMAKE_JS_LIB})
    endif()

    # Target N-API version 8 (Node 12.22+, Bun compatible)
    target_compile_definitions(arkilian_napi PRIVATE NAPI_VERSION=8)
endif()

# Install rules
include(GNUInstallDirs)

if(ARKILIAN_BUILD_SHARED)
    install(TARGETS arkilian
        EXPORT arkilianTargets
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/arkilian
    )
endif()

if(ARKILIAN_BUILD_STATIC)
    install(TARGETS arkilian_static
        EXPORT arkilianTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/arkilian
    )
endif()

# CMake config for find_package
install(EXPORT arkilianTargets
    FILE arkilianTargets.cmake
    NAMESPACE arkilian::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/arkilian
)

# Create a simple config file
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/arkilianConfig.cmake
"include(\${CMAKE_CURRENT_LIST_DIR}/arkilianTargets.cmake)
")
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/arkilianConfigVersion.cmake
"set(PACKAGE_VERSION \"1.0.0\")
if(\"\${PACKAGE_FIND_VERSION}\" VERSION_LESS_EQUAL \"\${PACKAGE_VERSION}\")
  set(PACKAGE_VERSION_COMPATIBLE TRUE)
else()
  set(PACKAGE_VERSION_COMPATIBLE FALSE)
endif()
if(\"\${PACKAGE_FIND_VERSION}\" VERSION_EQUAL \"\${PACKAGE_VERSION}\")
  set(PACKAGE_VERSION_EXACT TRUE)
endif()
")

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/arkilianConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/arkilianConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/arkilian
)

# Tests
if(ARKILIAN_BUILD_TESTS)
    if(ARKILIAN_BUILD_STATIC)
        add_executable(test_basic tests/test_basic.c)
        target_link_libraries(test_basic PRIVATE arkilian_static)
        add_test(NAME BasicTests COMMAND test_basic)
        add_executable(bench_1m tests/bench_1m.c)
        target_link_libraries(bench_1m PRIVATE arkilian_static ${CMAKE_THREAD_LIBS_INIT} ${CURL_LIBRARIES} m)
    endif()
endif()
