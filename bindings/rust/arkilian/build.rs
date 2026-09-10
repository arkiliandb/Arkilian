fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/bindings.h");

    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()));
    if let Ok(dir) = std::env::var("ARKILIAN_LIB_DIR") {
        println!("cargo:rustc-link-search=native={}", dir);
    }
    if let Ok(p) = manifest_dir.join("../../../build").canonicalize() {
        println!("cargo:rustc-link-search=native={}", p.display());
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", p.display());
    }
    if let Ok(p) = manifest_dir.join("../../../build/Release").canonicalize() {
        println!("cargo:rustc-link-search=native={}", p.display());
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", p.display());
    }
    println!("cargo:rustc-link-search=native=/usr/local/lib");
    println!("cargo:rustc-link-search=native=/opt/homebrew/lib");
    println!("cargo:rustc-link-lib=arkilian");
    println!("cargo:rustc-link-lib=curl");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "linux" {
        println!("cargo:rustc-link-lib=pthread");
        println!("cargo:rustc-link-lib=m");
        println!("cargo:rustc-link-lib=dl");
    } else if target_os == "macos" {
        println!("cargo:rustc-link-lib=pthread");
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
        println!("cargo:rustc-link-lib=framework=Security");
    }
    
    let bindings = bindgen::Builder::default()
        .header("src/bindings.h")
        .allowlist_type("arkilian")
        .allowlist_function("db_.*")
        .generate()
        .expect("Unable to generate bindings");
    
    bindings.write_to_file("src/bindings.rs")
        .expect("Failed to write bindings");
}