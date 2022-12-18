use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::time::Instant;

use arrow::chunk::Chunk;

use arrow::error::Result;

use pa::read::reader::{infer_schema, read_meta, PaReader};
use pa::{ColumnMeta};

/// Simplest way: read all record batches from the file. This can be used e.g. for random access.
// cargo run --package pa --example pa_file_read  --release /tmp/input.pa
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

        let metas: Vec<ColumnMeta> = read_meta(&mut reader)?;

        let mut readers = vec![];
        for (meta, field) in metas.iter().zip(schema.fields.iter()) {
            let mut reader = File::open(file_path).unwrap();
            reader.seek(std::io::SeekFrom::Start(meta.offset)).unwrap();
            let reader = reader.take(meta.length);

            let buffer_size = meta.length.min(8192) as usize;
            let reader = BufReader::with_capacity(buffer_size, reader);
            let scratch = Vec::with_capacity(8 * 1024);

            let pa_reader = PaReader::new(
                reader,
                field.data_type().clone(),
                true,
                meta.num_values as usize,
                scratch,
            );

            readers.push(pa_reader);
        }

        'FOR: loop {
            let mut chunks = Vec::new();
            for reader in readers.iter_mut() {
                if !reader.has_next() {
                    break 'FOR;
                }
                chunks.push(reader.next_array().unwrap());
            }

            let chunk = Chunk::new(chunks);
            println!("READ -> {:?} rows", chunk.len());
        }
    }

    println!("cost {:?} ms", t.elapsed().as_millis());
    // println!("{}", print::write(&[chunks], &["names", "tt", "3", "44"]));
    Ok(())
}
