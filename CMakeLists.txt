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

# Compile-time SQLite options required by Arkilian. The preupdate hook
# drives deterministic-capture tests; FTS5 must be enabled in the
# amalgamation itself (not only the test TU) so virtual-table schemas
# actually compile on the resulting library.
set(ARKILIAN_SQLITE_DEFS
    SQLITE_ENABLE_PREUPDATE_HOOK
    SQLITE_ENABLE_FTS5
    SQLITE_ENABLE_RTREE
    SQLITE_ENABLE_DBSTAT_VTAB)

# Shared Library
if(ARKILIAN_BUILD_SHARED)
    add_library(arkilian SHARED ${ARKILIAN_SOURCES})
    target_compile_definitions(arkilian PRIVATE ${ARKILIAN_SQLITE_DEFS})
    target_include_directories(arkilian PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/deps/sqlite>
        $<INSTALL_INTERFACE:include/arkilian>
    )
    target_link_libraries(arkilian PRIVATE CURL::libcurl Threads::Threads)
    set_target_properties(arkilian PROPERTIES
        VERSION 1.1.0
        SOVERSION 1
        PUBLIC_HEADER "src/class.h;src/deps/sqlite/sqlite3.h"
    )
endif()

# Static Library
if(ARKILIAN_BUILD_STATIC)
    add_library(arkilian_static STATIC ${ARKILIAN_SOURCES})
    target_compile_definitions(arkilian_static PRIVATE ${ARKILIAN_SQLITE_DEFS})
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
    target_compile_definitions(arkilian_napi PRIVATE ${ARKILIAN_SQLITE_DEFS})

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
"set(PACKAGE_VERSION \"1.1.0\")
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
# Every test file in tests/ is wired as its own executable + ctest entry,
# so a regression in any one of them is visible individually rather than
# hidden behind a single runner. enable_testing() makes `ctest` discover
# them; ARKILIAN_BUILD_TESTS=ON is required (OFF by default to keep the
# default build lean). The bench/stress programs are NOT registered as
# ctest entries — they are correctness-asserting benchmarks but not part
# of the pass/fail gate (see their own assertions).
if(ARKILIAN_BUILD_TESTS)
    enable_testing()
    if(ARKILIAN_BUILD_STATIC)
        # Common link flags: pthreads + libcurl + libm. hydration tests also
        # need the standalone sha256 + hydration sources (they don't link
        # the static lib because hydration ships as its own TU).
        set(ARK_TEST_LIBS arkilian_static ${CMAKE_THREAD_LIBS_INIT} ${CURL_LIBRARIES} m)

        # ── Tests that link the static lib (class.c is in arkilian_static) ──
        foreach(t test_basic test_interception test_regressions test_kill_switch
                 test_kill_resilience test_load_contention test_monitoring
                 test_deterministic test_virtual_tables test_hardening)
            add_executable(${t} tests/${t}.c)
            target_link_libraries(${t} PRIVATE ${ARK_TEST_LIBS})
            add_test(NAME ${t} COMMAND ${t})
        endforeach()

        # FTS5 is already compiled into the static lib via
        # ARKILIAN_SQLITE_DEFS; the test needs no extra define.

        # ── Hydration tests: build hydration.c + sha256.c + sqlite3.c ──
        add_executable(test_hydration tests/test_hydration.c
                       src/hydration.c src/sha256.c src/deps/sqlite/sqlite3.c)
        target_include_directories(test_hydration PRIVATE src src/deps/sqlite)
        target_link_libraries(test_hydration PRIVATE ${CMAKE_THREAD_LIBS_INIT}
                              ${CURL_LIBRARIES})
        add_test(NAME test_hydration COMMAND test_hydration)

        # ── SHA-256 NIST vectors ──
        add_executable(test_sha256 tests/test_sha256.c src/sha256.c)
        target_include_directories(test_sha256 PRIVATE src)
        add_test(NAME test_sha256 COMMAND test_sha256)

        # ── e2e/stress tests that need a live control plane: built but NOT
        # registered with ctest (they require external services). Compile
        # them so a build breakage is caught; run them out-of-band via
        # ARKILIAN_HYDRATION_URL=… ctest -R e2e — or just invoke manually. ──
        add_executable(test_e2e_stress tests/test_e2e_stress.c
                       src/class.c src/deps/sqlite/sqlite3.c)
        target_include_directories(test_e2e_stress PRIVATE src src/deps/sqlite)
        target_link_libraries(test_e2e_stress PRIVATE ${CMAKE_THREAD_LIBS_INIT}
                              ${CURL_LIBRARIES})

        add_executable(test_minio_hydrate tests/test_minio_hydrate.c
                       src/hydration.c src/sha256.c src/deps/sqlite/sqlite3.c)
        target_include_directories(test_minio_hydrate PRIVATE src src/deps/sqlite)
        target_link_libraries(test_minio_hydrate PRIVATE ${CMAKE_THREAD_LIBS_INIT}
                              ${CURL_LIBRARIES})

        # ── Benchmarks / stress (built, NOT ctest-registered): assert
        # correctness internally but are too long-running for the gate. ──
        add_executable(bench_1m tests/bench_1m.c)
        target_link_libraries(bench_1m PRIVATE ${ARK_TEST_LIBS})
        target_compile_definitions(bench_1m PRIVATE ${ARKILIAN_SQLITE_DEFS})

        add_executable(stress_200m tests/stress_200m.c
                       src/class.c src/deps/sqlite/sqlite3.c)
        target_include_directories(stress_200m PRIVATE src src/deps/sqlite)
        target_link_libraries(stress_200m PRIVATE ${CMAKE_THREAD_LIBS_INIT}
                              ${CURL_LIBRARIES} m)
    endif()
endif()
