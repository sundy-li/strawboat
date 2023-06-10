// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{env, error::Error, fs, path::PathBuf, str::FromStr};

pub fn parquet_test_data() -> String {
    match get_data_dir("PARQUET_TEST_DATA", "../parquet-testing/data") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get parquet data dir: {err}"),
    }
}

/// Returns a directory path for finding test data.
///
/// udf_env: name of an environment variable
///
/// submodule_dir: fallback path (relative to CARGO_MANIFEST_DIR)
///
///  Returns either:
/// The path referred to in `udf_env` if that variable is set and refers to a directory
/// The submodule_data directory relative to CARGO_MANIFEST_PATH
fn get_data_dir(udf_env: &str, submodule_data: &str) -> Result<PathBuf, Box<dyn Error>> {
    // Try user defined env.
    if let Ok(dir) = env::var(udf_env) {
        let trimmed = dir.trim().to_string();
        if !trimmed.is_empty() {
            let pb = PathBuf::from(trimmed);
            if pb.is_dir() {
                return Ok(pb);
            } else {
                return Err(format!(
                    "the data dir `{}` defined by env {} not found",
                    pb.display(),
                    udf_env
                )
                .into());
            }
        }
    }

    // The env is undefined or its value is trimmed to empty, let's try default dir.

    // env "CARGO_MANIFEST_DIR" is "the directory containing the manifest of your package",
    // set by `cargo run` or `cargo test`, see:
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let dir = env!("CARGO_MANIFEST_DIR");

    let pb = PathBuf::from(dir).join(submodule_data);
    if pb.is_dir() {
        Ok(pb)
    } else {
        Err(format!(
            "env `{}` is undefined or has empty value, and the pre-defined data dir `{}` not found\n\
             HINT: try running `git submodule update --init`",
            udf_env,
            pb.display(),
        ).into())
    }
}

/// Returns path to the test parquet file in 'data' directory
pub fn get_test_path(file_name: &str) -> PathBuf {
    let mut pathbuf = PathBuf::from_str(&parquet_test_data()).unwrap();
    pathbuf.push(file_name);
    pathbuf
}

/// Returns file handle for a test parquet file from 'data' directory
pub fn get_test_file(file_name: &str) -> fs::File {
    let path = get_test_path(file_name);
    fs::File::open(path.as_path()).unwrap_or_else(|err| {
        panic!(
            "Test file {} could not be opened, did you do `git submodule update`?: {}",
            path.display(),
            err
        )
    })
}
