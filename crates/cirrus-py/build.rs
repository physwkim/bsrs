//! macOS / pyo3 linker quirk: extension modules expect Python's
//! symbols to be resolved at load time, not link time. Without this,
//! `cargo build -p cirrus-py` on macOS fails with "ld: symbol(s) not
//! found" for the Py_* / _Py_* exports. The flags below tell the
//! linker to leave those symbols dynamic, which Python then resolves
//! when it loads the .so/.dylib at runtime.
//!
//! Linux + Windows pick up the right behavior via pyo3's own build
//! script.

fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-link-arg=-undefined");
        println!("cargo:rustc-link-arg=dynamic_lookup");
    }
}
