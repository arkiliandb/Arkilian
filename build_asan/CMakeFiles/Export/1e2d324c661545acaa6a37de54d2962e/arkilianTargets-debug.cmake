#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "arkilian::arkilian" for configuration "Debug"
set_property(TARGET arkilian::arkilian APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(arkilian::arkilian PROPERTIES
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/libarkilian.1.0.0.dylib"
  IMPORTED_SONAME_DEBUG "@rpath/libarkilian.1.dylib"
  )

list(APPEND _cmake_import_check_targets arkilian::arkilian )
list(APPEND _cmake_import_check_files_for_arkilian::arkilian "${_IMPORT_PREFIX}/lib/libarkilian.1.0.0.dylib" )

# Import target "arkilian::arkilian_static" for configuration "Debug"
set_property(TARGET arkilian::arkilian_static APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(arkilian::arkilian_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "C"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/libarkilian.a"
  )

list(APPEND _cmake_import_check_targets arkilian::arkilian_static )
list(APPEND _cmake_import_check_files_for_arkilian::arkilian_static "${_IMPORT_PREFIX}/lib/libarkilian.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
