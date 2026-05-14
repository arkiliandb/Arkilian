fn main() {
    println!("cargo:rerun-if-changed=src/bindings.h");
    println!("cargo:rustc-link-search=../../../build/Release");
    println!("cargo:rustc-link-lib=arkilian");
    
    let bindings = bindgen::Builder::default()
        .header("src/bindings.h")
        .allowlist_type("arkilian")
        .allowlist_function("db_.*")
        .generate()
        .expect("Unable to generate bindings");
    
    bindings.write_to_file("src/bindings.rs")
        .expect("Failed to write bindings");
}