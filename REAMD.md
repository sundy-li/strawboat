# PA

A native storage format based on arrow



## Examples 

```
// you need a simple parquet file in /tmp/input.parquet

// then generate pa file
cargo run --package pa --example pa_file_write --release /tmp/input.pa     

// read pa file
cargo run --package pa --example pa_file_read  --release /tmp/input.pa

// compare parquet reader
cargo run --package pa --example parquet_read --release /tmp/input.parquet   
```