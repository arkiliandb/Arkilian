fn main() {
    // Compile arkilian + sqlite3 into a static library
    // Note: sqlite3 and class.c are compiled separately to avoid
    // define conflicts with macOS system headers.

    // 1. Compile sqlite3.c (no _POSIX_C_SOURCE — it conflicts with macOS headers)
    cc::Build::new()
        .file("../../../src/deps/sqlite/sqlite3.c")
        .include("../../../src/deps/sqlite")
        .warnings(false) // sqlite3.c has many warnings
        .compile("sqlite3_embedded");

    // 2. Compile class.c (needs _POSIX_C_SOURCE for setenv/strdup)
    let mut build = cc::Build::new();
    build
        .file("../../../src/class.c")
        .include("../../../src")
        .include("../../../src/deps/sqlite")
        .warnings(false);

    // Only define _POSIX_C_SOURCE on Linux (macOS doesn't need it for setenv)
    if cfg!(target_os = "linux") {
        build.define("_POSIX_C_SOURCE", "200809L");
    }

    if cfg!(target_os = "windows") {
        build.define("_CRT_SECURE_NO_WARNINGS", None);
    }

    build.compile("arkilian_core");

    // Link system libraries
    println!("cargo:rustc-link-lib=curl");

    if cfg!(target_os = "linux") {
        println!("cargo:rustc-link-lib=pthread");
        println!("cargo:rustc-link-lib=dl");
        println!("cargo:rustc-link-lib=m");
    } else if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-lib=pthread");
    }

    // Rebuild if sources change
    println!("cargo:rerun-if-changed=../../../src/class.c");
    println!("cargo:rerun-if-changed=../../../src/class.h");
    println!("cargo:rerun-if-changed=../../../src/deps/sqlite/sqlite3.c");
}
