cmake_minimum_required(VERSION 3.10 FATAL_ERROR)

project(unbox C)

set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_MSVC_RUNTIME_LIBRARY MultiThreaded)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

if(APPLE OR LINUX)
    set(CFLAGS -Wall -Wextra -Wpedantic -Werror)
    set(LFLAGS)
elseif(WIN32)
    set(CFLAGS
        /Wall
        /WX
        /wd4061 # -Wno-switch
        /wd4267 # -Wno-conversion
        /wd4456 # -Wno-shadow
        /wd4464 #  relative include path contains '..'
        /wd4668 # -Wno-undef
        /wd4710 # function not inlined
        /wd4711 # function selected for automatic inline expansion
        /wd4820 # -Wno-padded
        /wd5045 #  Compiler will insert Spectre mitigation for memory load if /Qspectre switch specified
    )
    set(LFLAGS)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    if(LINUX)
        list(APPEND CFLAGS -fsanitize=address)
        list(APPEND LFLAGS -fsanitize=address)
    elseif(APPLE)
        list(APPEND CFLAGS
            -fsanitize=undefined
            -Weverything
            -Wno-alloca
            -Wno-bad-function-cast
            -Wno-cast-align
            -Wno-cast-function-type-strict
            -Wno-cast-qual
            -Wno-comma
            -Wno-conditional-uninitialized
            -Wno-covered-switch-default
            -Wno-declaration-after-statement
            -Wno-disabled-macro-expansion
            -Wno-documentation
            -Wno-documentation-unknown-command
            -Wno-double-promotion
            -Wno-float-equal
            -Wno-format
            -Wno-format-nonliteral
            -Wno-implicit-int-conversion
            -Wno-missing-noreturn
            -Wno-missing-prototypes
            -Wno-missing-variable-declarations
            -Wno-padded -Wno-poison-system-directories
            -Wno-shadow
            -Wno-shorten-64-to-32
            -Wno-sign-conversion
            -Wno-switch-default
            -Wno-switch-enum
            -Wno-undef
            -Wno-unreachable-code-break
            -Wno-unused-macros
        )
        list(APPEND LFLAGS -fsanitize=undefined)
    endif()
endif()

function(add_flags target)
    if(WIN32)
        target_compile_definitions(${target} PRIVATE _CRT_SECURE_NO_WARNINGS _CRT_NO_POSIX_ERROR_CODES)
    endif()
    target_compile_options(${target} PRIVATE ${CFLAGS})
    target_link_options(${target} PRIVATE ${LFLAGS})
endfunction()


add_subdirectory(dep/sqlite EXCLUDE_FROM_ALL)
add_subdirectory(dep/raylib EXCLUDE_FROM_ALL)


add_executable(sha1 dev/sha1.c)
add_flags(sha1)
target_link_libraries(sha1 sqlite)

get_property(SQLITE_INCLUDES TARGET sqlite PROPERTY INCLUDE_DIRECTORIES)
list(JOIN SQLITE_INCLUDES ") X(" SQLITE_INCLUDES_STR)
get_property(UNBOXING_INCLUDES TARGET unboxing PROPERTY INCLUDE_DIRECTORIES)
list(JOIN UNBOXING_INCLUDES ") X(" UNBOXING_INCLUDES_STR)
get_property(MINIXML_INCLUDES TARGET minixml PROPERTY INCLUDE_DIRECTORIES)
list(JOIN MINIXML_INCLUDES ") X(" MINIXML_INCLUDES_STR)
set(INCLUDES "X(${SQLITE_INCLUDES_STR}) X(${UNBOXING_INCLUDES_STR}) X(${MINIXML_INCLUDES_STR})")
add_executable(metagen dev/metagen.c)
set_property(SOURCE dev/metagen.c APPEND PROPERTY COMPILE_DEFINITIONS INCLUDES=${INCLUDES})
add_flags(metagen)

add_custom_command(OUTPUT
    "${CMAKE_CURRENT_SOURCE_DIR}/.vscode/c_cpp_properties.json"
    "${CMAKE_CURRENT_SOURCE_DIR}/dev/doc_example_program.c"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMAND metagen
    DEPENDS doc/DETAILED.md
)

add_executable(doc_example_program dev/doc_example_program.c)
add_flags(doc_example_program)
target_include_directories(doc_example_program PRIVATE dep/sqlite/unboxing/tests/testutils/src)
target_link_libraries(doc_example_program sqlite)


add_executable(raw_file_to_png dev/raw_file_to_png.c)
add_flags(raw_file_to_png)
target_link_libraries(raw_file_to_png unboxing)


add_executable(unbox src/main.c)
add_flags(unbox)
target_link_libraries(unbox sqlite)


add_executable(raw_viewer dev/raw_viewer.c)
add_flags(raw_viewer)
target_link_libraries(raw_viewer raylib)


enable_testing()

add_test(
    NAME unbox
    COMMAND unbox dep/ivm_testdata/reel/png out/data
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)
add_test(
    NAME doctest
    COMMAND doc_example_program
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)
