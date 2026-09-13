use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    
    // Find target/debug or target/release directory
    let mut target_dir = out_dir.clone();
    while target_dir.file_name().map(|s| s != "target").unwrap_or(false) {
        if !target_dir.pop() {
            break;
        }
    }
    
    // Source library in examples/c/lib or build/
    let lib_candidates = [
        manifest_dir.join("../c/lib"),
        manifest_dir.join("../../build"),
    ];

    for lib_dir in &lib_candidates {
        if lib_dir.exists() {
            println!("cargo:rustc-link-search=native={}", lib_dir.display());
            println!("cargo:rustc-link-lib=dylib=arkilian");
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());

            // Symlink or copy to debug/release if found
            for profile in &["debug", "release"] {
                let dest_dir = target_dir.join(profile);
                if dest_dir.exists() {
                    let dylib_name = if cfg!(target_os = "macos") {
                        "libarkilian.1.dylib"
                    } else if cfg!(target_os = "windows") {
                        "arkilian.dll"
                    } else {
                        "libarkilian.so.1"
                    };

                    let src = lib_dir.join(if cfg!(target_os = "macos") { "libarkilian.dylib" } else { dylib_name });
                    let dst = dest_dir.join(dylib_name);
                    if src.exists() && !dst.exists() {
                        let _ = fs::copy(&src, &dst);
                    }
                }
            }
            break;
        }
    }
}
