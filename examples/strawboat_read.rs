use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::time::Instant;

use arrow::chunk::Chunk;
use arrow::io::parquet::read::n_columns;
use arrow::io::parquet::write::to_parquet_schema;

use arrow::error::Result;

use strawboat::read::reader::{infer_schema, is_primitive, read_meta, NativeReader};
use strawboat::ColumnMeta;

/// Simplest way: read all record batches from the file. This can be used e.g. for random access.
// cargo run --example strawboat_file_read  --release /tmp/input.st
fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let t = Instant::now();
    {
        let mut reader = File::open(file_path).unwrap();
        // we can read its metadata:
        // and infer a [`Schema`] from the `metadata`.
        let schema = infer_schema(&mut reader).unwrap();

        let mut metas: Vec<ColumnMeta> = read_meta(&mut reader)?;
        let schema_descriptor = to_parquet_schema(&schema)?;
        let mut leaves = schema_descriptor.columns().to_vec();

        let mut readers = vec![];
        for field in schema.fields.iter() {
            let n = n_columns(&field.data_type);

            let curr_metas: Vec<ColumnMeta> = metas.drain(..n).collect();
            let curr_leaves = leaves.drain(..n).collect();

            let mut page_readers = Vec::with_capacity(n);
            let mut scratchs = Vec::with_capacity(n);
            for curr_meta in curr_metas.iter() {
                let mut reader = File::open(file_path).unwrap();
                reader
                    .seek(std::io::SeekFrom::Start(curr_meta.offset))
                    .unwrap();
                let reader = reader.take(curr_meta.total_len());
                let buffer_size = curr_meta.total_len().min(8192) as usize;
                let reader = BufReader::with_capacity(buffer_size, reader);
                page_readers.push(reader);

                let scratch = Vec::with_capacity(8 * 1024);
                scratchs.push(scratch);
            }

            let is_nested = is_primitive(field.data_type());
            let pa_reader = NativeReader::new(
                page_readers,
                field.clone(),
                is_nested,
                curr_leaves,
                curr_metas,
                scratchs,
            );
            readers.push(pa_reader);
        }

        'FOR: loop {
            let mut arrays = Vec::new();
            for reader in readers.iter_mut() {
                if !reader.has_next() {
                    break 'FOR;
                }
                arrays.push(reader.next_array().unwrap());
            }

            let chunk = Chunk::new(arrays);
            println!("READ -> {:?} rows", chunk.len());
        }
    }
    println!("cost {:?} ms", t.elapsed().as_millis());
    // println!("{}", print::write(&[chunks], &["names", "tt", "3", "44"]));
    Ok(())
}
