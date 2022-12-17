use std::io::{BufRead, Read};

use arrow::{
    array::{Array, UInt32Array, PrimitiveArray, BooleanArray},
    chunk::Chunk,
    datatypes::{Field, Schema}, compute,
};
use pa::{
    read::reader::PaReader,
    write::{PaWriter, WriteOptions},
};
use rand::{rngs::StdRng, SeedableRng, Rng};


#[test]
fn test_basic1() {
    let chunk = Chunk::new(vec![Box::new(UInt32Array::from_vec(vec![1,2,3,4,5,6])) as _]);
    test_write_read(chunk, WriteOptions { compression: None, max_page_size: Some(12) });
}

#[test]
fn test_random() {
    let size = 1000;
    let chunk = Chunk::new(vec![
        Box::new(create_random_index(size, 0.1)) as _,
        Box::new(create_random_index(size, 0.2)) as _,
        Box::new(create_random_index(size, 0.3)) as _,
        Box::new(create_random_index(size, 0.4)) as _,
        Box::new(create_random_index(size, 0.5)) as _,
    ]);
    test_write_read(chunk, WriteOptions { compression: None, max_page_size: Some(12) });
}

// #[test]
// fn test_boolean() {
//     let chunk = Chunk::new(vec![Box::new(BooleanArray::from_slice(&[true])) as _]);
//     test_write_read(chunk, WriteOptions { compression: Some(pa::write::Compression::LZ4), max_page_size: Some(12) });
// }

fn create_random_index(size: usize, null_density: f32) -> PrimitiveArray<i32> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() > null_density {
                let value = rng.gen_range::<i32, _>(0i32..size as i32);
                Some(value)
            } else {
                None
            }
        })
        .collect::<PrimitiveArray<i32>>()
}

fn test_write_read(chunk: Chunk<Box<dyn Array>>, options: WriteOptions) {
    let mut bytes = Vec::new();
    let fields: Vec<Field> = chunk
        .iter()
        .map(|array| Field::new("name", array.data_type().clone(), array.validity().is_some()))
        .collect();

    let schema = Schema::from(fields);
    let mut writer = PaWriter::new(&mut bytes, schema.clone(), options);

    writer.start().unwrap();
    writer.write(&chunk).unwrap();
    writer.finish().unwrap();
    
    let mut results = Vec::with_capacity(chunk.len());
    
    for (meta, field) in writer.metas.iter().zip(schema.fields.iter()) {
        let mut range_bytes = std::io::Cursor::new(bytes.clone());
        range_bytes.consume(meta.offset as usize);
        
        let compression = match options.compression {
            Some(c) => match c {
                pa::write::Compression::LZ4 => Some(pa::read::Compression::LZ4),
                pa::write::Compression::ZSTD => Some(pa::read::Compression::ZSTD),
            }
            None => None,
        };
        let mut reader = PaReader::new(
            range_bytes,
            field.data_type().clone(),
            true,
            compression,
            meta.num_values as usize,
            Vec::new(),
        );
        
        let mut vs = vec![];
        loop {
            if !reader.has_next()  {
                break;
            }
            vs.push(reader.next_array().unwrap());
        }   
        let vs: Vec<&dyn Array> = vs.iter().map(|v| v.as_ref()).collect();
        let result = compute::concatenate::concatenate(&vs).unwrap();
        results.push(result);
    }
    
    let result_chunk = Chunk::new(results);
    assert_eq!(chunk, result_chunk);
}

 