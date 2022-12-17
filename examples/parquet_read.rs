use std::fs::File;
use std::time::SystemTime;

use arrow::error::Error;
use arrow::io::parquet::read;

// cargo run --package pa --example parquet_read --release /tmp/input.parquet        
fn main() -> Result<(), Error> {
    // say we have a file
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let mut reader = File::open(file_path)?;

    // we can read its metadata:
    let metadata = read::read_metadata(&mut reader)?;

    // and infer a [`Schema`] from the `metadata`.
    let schema = read::infer_schema(&metadata)?;

    // we can filter the columns we need (here we select all)
    let schema = schema.filter(|_index, _field| true);

    // we can read the statistics of all parquet's row groups (here for each field)
    for field in &schema.fields {
        let statistics = read::statistics::deserialize(field, &metadata.row_groups)?;
        println!("{:#?}", statistics);
    }

    // say we found that we only need to read the first two row groups, "0" and "1"
    let row_groups = metadata
        .row_groups
        .into_iter()
        .enumerate()
        .filter(|(index, _)| *index == 0 || *index == 1)
        .map(|(_, row_group)| row_group)
        .collect();

    // we can then read the row groups into chunks
    let chunks = read::FileReader::new(reader, row_groups, schema, Some(usize::MAX), None, None);

    let start = SystemTime::now();
    for maybe_chunk in chunks {
        let chunk = maybe_chunk?;
        assert!(!chunk.is_empty());

        println!("chunk len -> {:?}", chunk.len());
        // println!("{}", print::write(&[chunk], &["names"]));
    }
    println!("took: {} ms", start.elapsed().unwrap().as_millis());

    Ok(())
}
