use std::fs::File;
use std::io::{BufReader, Seek};
use std::time::Instant;

use arrow::{
    chunk::Chunk,
    error::Result,
    io::parquet::{
        read::{n_columns, ColumnDescriptor},
        write::to_parquet_schema,
    },
};
use strawboat::{
    read::{
        batch_read::batch_read_array,
        reader::{infer_schema, is_primitive, read_meta},
    },
    ColumnMeta, PageMeta,
};

/// Simplest way: read all record batches from the file. This can be used e.g. for random access.
// cargo run --example strawboat_batch_read --release /tmp/input.str
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

        let mut arrays = vec![];
        for field in schema.fields.iter() {
            let n = n_columns(&field.data_type);

            let curr_metas: Vec<ColumnMeta> = metas.drain(..n).collect();
            let curr_leaves: Vec<ColumnDescriptor> = leaves.drain(..n).collect();

            let mut pages: Vec<Vec<PageMeta>> = Vec::with_capacity(n);
            let mut readers = Vec::with_capacity(n);
            for curr_meta in curr_metas.iter() {
                pages.push(curr_meta.pages.clone());
                let mut reader = File::open(file_path).unwrap();
                reader
                    .seek(std::io::SeekFrom::Start(curr_meta.offset))
                    .unwrap();
                let buffer_size = curr_meta.total_len().min(8192) as usize;
                let reader = BufReader::with_capacity(buffer_size, reader);

                readers.push(reader);
            }
            let is_nested = !is_primitive(field.data_type());

            let array = batch_read_array(readers, curr_leaves, field.clone(), is_nested, pages)?;
            arrays.push(array);
        }

        let chunk = Chunk::new(arrays);
        println!("chunk={chunk:?}");
        println!("READ -> {:?} rows", chunk.len());
    }
    println!("cost {:?} ms", t.elapsed().as_millis());
    Ok(())
}
