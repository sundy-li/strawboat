# strawboat

A native storage format based on [Apache Arrow](https://arrow.apache.org/).

strawboat is similar to [Arrow IPC](https://arrow.apache.org/docs/python/ipc.html) and is primarily aimed at optimizing the storage layer. We hope to use it in [databend](https://github.com/datafuselabs/databend) as another storage_format, and it is currently in a very early stage.


## Difference with parquet

* No RowGroup, row based multiple pages.

We think that multi `RowGroup` per file is useless in columnar database. To make life easier, we did not implement RowGroup (just like one single RowGroup per file).

Each Column will be spilted into fixed row size `Page` by WriteOptions. `Page` is the smallest unit of compression like parquet.



* Zero-Overhead reading and writing. 




* No encoding/decoding.

Encoding/Decoding in parquet may introduce overhead when reading from storage. We hope that the memory format of our data can be easily and efficiently converted to and from the storage format, just like IPC. It is possible to combine encoding and compression functionality, as long as you have implemented a similar compression algorithm.

## Storage Format Layout

We have three different data page layouts to store different types of data.

Non-Nullable data pages:
```
+-------------------+
|    codec type     |
+-------------------+
|  compressed size  |
+-------------------+
| uncompressed size |
+-------------------+
|     values        |
+-------------------+
```

Nullable data pages:
```
+-------------------+
|  def levels len   |
+-------------------+
|    def values     |
+-------------------+
|    codec type     |
+-------------------+
|  compressed size  |
+-------------------+
| uncompressed size |
+-------------------+
|     values        |
+-------------------+
```

Nested data pages:
```
+-------------------+
|    offsets len    |
+-------------------+
|  rep levels len   |
+-------------------+
|  def levels len   |
+-------------------+
|    rep values     |
+-------------------+
|    def values     |
+-------------------+
|    codec type     |
+-------------------+
|  compressed size  |
+-------------------+
| uncompressed size |
+-------------------+
|     values        |
+-------------------+
```

## DataTypes

- [x] Boolean
- [x] Primitive 
- [x] Binary/Utf8 
- [x] Null
- [x] List 
- [x] LargeList 
- [ ] Fixed sized binary
- [ ] Fixed sized list
- [x] Struct 
- [ ] Dictionary
- [ ] Union
- [ ] Map


## Performance compare with parquet

TODO

## Examples 

```
// you need a simple parquet file in /tmp/input.str

// then generate strawboat file
cargo run --example strawboat_write --release /tmp/input.str

// read starwboat file
cargo run --example strawboat_read  --release /tmp/input.str

// compare parquet reader
cargo run --example parquet_read --release /tmp/input.str
```

