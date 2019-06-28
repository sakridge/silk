use std::env;

fn main() {
    if env::var("CARGO_FEATURE_NVTX").is_ok() {
        println!("cargo:rustc-link-search=native={}/lib64", "/usr/local/cuda");
    }
}
