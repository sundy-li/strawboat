
use arrow::offset::OffsetsBuffer;
use arrow::{
    array::{
        Array, BooleanArray, Int32Array, ListArray, PrimitiveArray, StructArray, UInt32Array,
        Utf8Array,
    },
    chunk::Chunk,
    compute,
    datatypes::{DataType, Field, Schema},
};
use pa::{
    read::reader::PaReader,
    write::{PaWriter, WriteOptions},
    Compression,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::io::BufRead;

#[test]
fn test_basic1() {
    let chunk = Chunk::new(vec![
        Box::new(UInt32Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
    ]);
    test_write_read(
        chunk,
        WriteOptions {
            compression: Compression::ZSTD,
            max_page_size: Some(12),
        },
    );
}

#[test]
fn test_random_nonull() {
    let size = 2000;
    let chunk = Chunk::new(vec![
        Box::new(create_random_index(size, 0.0)) as _,
        Box::new(create_random_index(size, 0.0)) as _,
        Box::new(create_random_index(size, 0.0)) as _,
        Box::new(create_random_index(size, 0.0)) as _,
        Box::new(create_random_index(size, 0.0)) as _,
    ]);
    test_write_read(
        chunk,
        WriteOptions {
            compression: Compression::None,
            max_page_size: Some(12),
        },
    );
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
    test_write_read(
        chunk,
        WriteOptions {
            compression: Compression::ZSTD, //TODO: Not  work in Compression::None
            max_page_size: Some(12),
        },
    );
}

#[test]
fn test_boolean() {
    let chunk = Chunk::new(vec![Box::new(BooleanArray::from_slice(&[true])) as _]);
    test_write_read(
        chunk,
        WriteOptions {
            compression: Compression::LZ4,
            max_page_size: Some(12),
        },
    );
}

#[test]
fn test_struct() {
    let s1 = [Some("a"), Some("bc"), None];
    let s2 = [Some(1), Some(2), None];
    let dt = DataType::Struct(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ]);
    let struct_array = StructArray::try_new(
        dt,
        vec![
            Utf8Array::<i32>::from(s1).boxed(),
            Int32Array::from(s2).boxed(),
        ],
        None,
    )
    .unwrap();
    let chunk = Chunk::new(vec![Box::new(struct_array) as _]);
    test_write_read(
        chunk,
        WriteOptions {
            compression: Compression::LZ4,
            max_page_size: Some(12),
        },
    );
}

#[test]
fn test_list() {
    let l1 = Int32Array::from(&[
        Some(0),
        Some(1),
        None,
        Some(2),
        Some(3),
        None,
        Some(4),
        Some(5),
        None,
    ]);
    let list_array = ListArray::try_new(
        DataType::List(Box::new(Field::new("item", l1.data_type().clone(), false))),
        OffsetsBuffer::try_from(vec![0, 3, 5, 9]).unwrap(),
        l1.boxed(),
        None,
    )
    .unwrap();
    let chunk = Chunk::new(vec![Box::new(list_array) as _]);
    test_write_read(
        chunk,
        WriteOptions {
            compression: Compression::LZ4,
            max_page_size: Some(12),
        },
    );
}

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
        .map(|array| {
            Field::new(
                "name",
                array.data_type().clone(),
                array.validity().is_some(),
            )
        })
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

        let mut reader = PaReader::new(
            range_bytes,
            field.data_type().clone(),
            true,
            meta.num_values as usize,
            Vec::new(),
        );

        let mut vs = vec![];
        loop {
            if !reader.has_next() {
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
