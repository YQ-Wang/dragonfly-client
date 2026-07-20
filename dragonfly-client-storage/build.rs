/*
 *     Copyright 2026 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::env;
use std::path::PathBuf;
use std::process::Command;

/// Locates the libfabric installation and compiles the C shim for the optional `rdma`
/// feature. The default build (feature disabled) does not touch libfabric at all.
fn main() {
    if env::var_os("CARGO_FEATURE_RDMA").is_none() {
        return;
    }

    println!("cargo:rerun-if-changed=src/rdma/shim.c");
    println!("cargo:rerun-if-env-changed=LIBFABRIC_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=LIBFABRIC_LIB_DIR");

    let (include_dir, lib_dir) = locate_libfabric();
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is set by cargo"));
    let obj = out_dir.join("dfrdma_shim.o");
    let archive = out_dir.join("libdfrdma_shim.a");

    let cc = env::var("CC").unwrap_or_else(|_| "cc".to_string());
    let mut compile = Command::new(&cc);
    compile
        .arg("-c")
        .arg("src/rdma/shim.c")
        .arg("-o")
        .arg(&obj)
        .arg("-O2")
        .arg("-fPIC")
        .arg("-Wall")
        .arg("-Werror");
    if let Some(ref include_dir) = include_dir {
        compile.arg(format!("-I{}", include_dir.display()));
    }
    let status = compile
        .status()
        .expect("failed to run the C compiler; the rdma feature requires a C toolchain");
    assert!(status.success(), "failed to compile src/rdma/shim.c");

    let status = Command::new(env::var("AR").unwrap_or_else(|_| "ar".to_string()))
        .arg("crs")
        .arg(&archive)
        .arg(&obj)
        .status()
        .expect("failed to run ar");
    assert!(status.success(), "failed to archive the rdma shim");

    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=dfrdma_shim");
    if let Some(ref lib_dir) = lib_dir {
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
    }
    println!("cargo:rustc-link-lib=dylib=fabric");
}

/// Resolves libfabric include and library directories from, in order: explicit environment
/// variables, pkg-config, and well-known installation prefixes.
fn locate_libfabric() -> (Option<PathBuf>, Option<PathBuf>) {
    let env_include = env::var_os("LIBFABRIC_INCLUDE_DIR").map(PathBuf::from);
    let env_lib = env::var_os("LIBFABRIC_LIB_DIR").map(PathBuf::from);
    if env_include.is_some() || env_lib.is_some() {
        return (env_include, env_lib);
    }

    if let Ok(output) = Command::new("pkg-config")
        .args(["--cflags-only-I", "--libs-only-L", "libfabric"])
        .output()
    {
        if output.status.success() {
            let flags = String::from_utf8_lossy(&output.stdout);
            let include = flags
                .split_whitespace()
                .find_map(|flag| flag.strip_prefix("-I").map(PathBuf::from));
            let lib = flags
                .split_whitespace()
                .find_map(|flag| flag.strip_prefix("-L").map(PathBuf::from));
            if include.is_some() || lib.is_some() {
                return (include, lib);
            }
        }
    }

    for prefix in [
        "/opt/homebrew/opt/libfabric",
        "/usr/local",
        "/opt/amazon/efa",
        "/usr",
    ] {
        let prefix = PathBuf::from(prefix);
        if prefix.join("include/rdma/fabric.h").exists() {
            let lib64 = prefix.join("lib64");
            let lib = if lib64.exists() {
                lib64
            } else {
                prefix.join("lib")
            };
            return (Some(prefix.join("include")), Some(lib));
        }
    }

    panic!(
        "the rdma feature requires libfabric; install it (e.g. apt install libfabric-dev, \
         brew install libfabric, or the AWS EFA installer) or set LIBFABRIC_INCLUDE_DIR and \
         LIBFABRIC_LIB_DIR"
    );
}
