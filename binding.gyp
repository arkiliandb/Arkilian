{
  "targets": [
    {
      "target_name": "arkilian",
      "sources": [
        "src/class.c",
        "src/hydration.c",
        "src/sha256.c",
        "src/deps/sqlite/sqlite3.c",
        "src/arkilian.cc"
      ],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")",
        "src",
        "src/deps/sqlite"
      ],
      "cflags!": ["-fno-exceptions"],
      "cflags_cc!": ["-fno-exceptions"],
      "cflags": ["-Wall", "-Wextra", "-Wpedantic", "-Werror"],
      "cflags_cc": ["-Wall", "-Wextra", "-Wpedantic", "-Werror"],
      "defines": [
        "NAPI_DISABLE_CPP_EXCEPTIONS",
        "SQLITE_ENABLE_PREUPDATE_HOOK",
        "SQLITE_ENABLE_FTS5",
        "SQLITE_ENABLE_RTREE",
        "SQLITE_ENABLE_DBSTAT_VTAB"
      ],
      "conditions": [
        ["OS=='mac'", {
          "xcode_settings": {
            "GCC_ENABLE_CPP_EXCEPTIONS": "YES",
            "CLANG_CXX_LIBRARY": "libc++",
            "MACOSX_DEPLOYMENT_TARGET": "10.15"
          },
          "libraries": ["-lcurl"]
        }],
        ["OS=='linux'", {
          "cflags_cc": ["-fPIC"],
          "libraries": ["-lcurl"]
        }],
        ["OS=='win'", {
          "defines": ["_CRT_SECURE_NO_WARNINGS"],
          "libraries": ["-lwinhttp", "-lws2_32", "-lcrypt32"],
          "msvs_settings": {
            "VCCLCompilerTool": {
              "ExceptionHandling": 1
            }
          },
          "include_dirs": [
            "deps/curl/include"
          ]
        }]
      ]
    }
  ]
}