{
  "targets": [
    {
      "target_name": "arkilian",
      "sources": [
        "src/class.c",
        "src/deps/sqlite/sqlite3.c",
        "src/arkilian.cc"
      ],
      "include_dirs": [
        "node_modules/node-addon-api",
        "src",
        "src/deps/sqlite"
      ],
      "cflags!": ["-fno-exceptions"],
      "cflags_cc!": ["-fno-exceptions"],
      "defines": ["NAPI_DISABLE_CPP_EXCEPTIONS"],
      "conditions": [
        ["OS=='mac'", {
          "xcode_settings": {
            "GCC_ENABLE_CPP_EXCEPTIONS": "YES",
            "CLANG_CXX_LIBRARY": "libc++",
            "MACOSX_DEPLOYMENT_TARGET": "10.15"
          }
        }],
        ["OS=='linux'", {
          "cflags_cc": ["-fPIC"]
        }]
      ]
    }
  ]
}