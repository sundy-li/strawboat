use std::io::Cursor;
use std::marker::PhantomData;

use crate::read::{read_basic::*, BufReader, NativeReadBuf, PageIterator};
use crate::{Compression, PageMeta};
use arrow::array::{Array, BinaryArray, Utf8Array};
use arrow::bitmap::{Bitmap, MutableBitmap};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::io::parquet::read::{InitNested, NestedState};
use arrow::offset::OffsetsBuffer;
use arrow::types::Offset;
use parquet2::metadata::ColumnDescriptor;

#[derive(Debug)]
pub struct BinaryIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    iter: I,
    is_nullable: bool,
    data_type: DataType,
    scratch: Vec<u8>,
    _phantom: PhantomData<O>,
}

impl<I, O> BinaryIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    pub fn new(iter: I, is_nullable: bool, data_type: DataType) -> Self {
        Self {
            iter,
            is_nullable,
            data_type,
            scratch: vec![],
            _phantom: PhantomData,
        }
    }
}

impl<I, O> BinaryIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    fn deserialize(&mut self, num_values: u64, buffer: Vec<u8>) -> Result<Box<dyn Array>> {
        let length = num_values as usize;
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let validity = if self.is_nullable {
            let mut validity_builder = MutableBitmap::with_capacity(length);
            read_validity(&mut reader, length, &mut validity_builder)?;
            Some(std::mem::take(&mut validity_builder).into())
        } else {
            None
        };

        let mut offsets: Vec<O> = Vec::with_capacity(length + 1);
        let mut values = Vec::with_capacity(0);
        read_binary_buffer(
            &mut reader,
            length,
            &mut self.scratch,
            &mut offsets,
            &mut values,
        )?;

        try_new_binary_array(
            self.data_type.clone(),
            unsafe { OffsetsBuffer::new_unchecked(offsets.into()) },
            values.into(),
            validity,
        )
    }
}

impl<I, O> Iterator for BinaryIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    type Item = Result<Box<dyn Array>>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self.iter.nth(n) {
            Some(Ok((num_values, buffer))) => Some(self.deserialize(num_values, buffer)),
            Some(Err(err)) => Some(Result::Err(err)),
            None => None,
        }
    }

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((num_values, buffer))) => Some(self.deserialize(num_values, buffer)),
            Some(Err(err)) => Some(Result::Err(err)),
            None => None,
        }
    }
}

#[derive(Debug)]
pub struct BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    iter: I,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
    _phantom: PhantomData<O>,
}

impl<I, O> BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    pub fn new(
        iter: I,
        data_type: DataType,
        leaf: ColumnDescriptor,
        init: Vec<InitNested>,
    ) -> Self {
        Self {
            iter,
            data_type,
            leaf,
            init,
            scratch: vec![],
            _phantom: PhantomData,
        }
    }
}

impl<I, O> BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    fn deserialize(
        &mut self,
        num_values: u64,
        buffer: Vec<u8>,
    ) -> Result<(NestedState, Box<dyn Array>)> {
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let (mut nested, validity) = read_validity_nested(
            &mut reader,
            num_values as usize,
            &self.leaf,
            self.init.clone(),
        )?;
        let length = nested.nested.pop().unwrap().len();

        let mut offsets: Vec<O> = Vec::with_capacity(length + 1);
        let mut values = Vec::with_capacity(0);
        read_binary_buffer(
            &mut reader,
            length,
            &mut self.scratch,
            &mut offsets,
            &mut values,
        )?;

        let array = try_new_binary_array(
            self.data_type.clone(),
            unsafe { OffsetsBuffer::new_unchecked(offsets.into()) },
            values.into(),
            validity,
        )?;
        Ok((nested, array))
    }
}

impl<I, O> Iterator for BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    type Item = Result<(NestedState, Box<dyn Array>)>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self.iter.nth(n) {
            Some(Ok((num_values, buffer))) => Some(self.deserialize(num_values, buffer)),
            Some(Err(err)) => Some(Result::Err(err)),
            None => None,
        }
    }

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((num_values, buffer))) => Some(self.deserialize(num_values, buffer)),
            Some(Err(err)) => Some(Result::Err(err)),
            None => None,
        }
    }
}

pub fn read_binary<O: Offset, R: NativeReadBuf>(
    reader: &mut R,
    is_nullable: bool,
    data_type: DataType,
    page_metas: Vec<PageMeta>,
) -> Result<Box<dyn Array>> {
    let num_values = page_metas.iter().map(|p| p.num_values as usize).sum();

    let total_length: usize = page_metas.iter().map(|p| p.length as usize).sum();

    let mut validity_builder = if is_nullable {
        Some(MutableBitmap::with_capacity(num_values))
    } else {
        None
    };
    let mut scratch = vec![];
    let out_off_len = num_values + 2;
    // don't know how much space is needed for the buffer,
    // if not enough, it may need to be reallocated several times.
    let out_buf_len = total_length * 4;
    let mut offsets: Vec<O> = Vec::with_capacity(out_off_len);
    let mut values: Vec<u8> = Vec::with_capacity(out_buf_len);

    for page_meta in page_metas {
        let length = page_meta.num_values as usize;
        if let Some(ref mut validity_builder) = validity_builder {
            read_validity(reader, length, validity_builder)?;
        }

        read_binary_buffer(reader, length, &mut scratch, &mut offsets, &mut values)?;
    }
    let validity =
        validity_builder.map(|mut validity_builder| std::mem::take(&mut validity_builder).into());
    let offsets: Buffer<O> = offsets.into();
    let values: Buffer<u8> = values.into();

    try_new_binary_array(
        data_type.clone(),
        unsafe { OffsetsBuffer::new_unchecked(offsets.into()) },
        values,
        validity,
    )
}

pub fn read_nested_binary<O: Offset, R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    page_metas: Vec<PageMeta>,
) -> Result<Vec<(NestedState, Box<dyn Array>)>> {
    let mut scratch = vec![];

    let mut results = Vec::with_capacity(page_metas.len());

    for page_meta in page_metas {
        let num_values = page_meta.num_values as usize;
        let (mut nested, validity) = read_validity_nested(reader, num_values, &leaf, init.clone())?;
        let length = nested.nested.pop().unwrap().len();

        let mut offsets: Vec<O> = Vec::with_capacity(length + 1);
        let mut values = Vec::with_capacity(0);
        read_binary_buffer(reader, length, &mut scratch, &mut offsets, &mut values)?;

        let array = try_new_binary_array(
            data_type.clone(),
            unsafe { OffsetsBuffer::new_unchecked(offsets.into()) },
            values.into(),
            validity,
        )?;

        results.push((nested, array));
    }
    Ok(results)
}

pub fn read_binary_buffer<O: Offset, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    scratch: &mut Vec<u8>,
    offsets: &mut Vec<O>,
    values: &mut Vec<u8>,
) -> Result<()> {
    let mut buf = vec![0u8; 1];
    let compression = Compression::from_codec(read_u8(reader, buf.as_mut_slice())?)?;
    let compressor = compression.create_compressor();

    if compressor.raw_mode() {
        let start_offset = offsets.last().copied();

        read_buffer(reader, 1 + length, scratch, offsets)?;
        read_buffer(reader, offsets.last().unwrap().to_usize(), scratch, values)?;
        // fix offset
        if let Some(start_offset) = start_offset {
            for i in offsets.len() - length - 1..offsets.len() - 1 {
                let next_val = unsafe { *offsets.get_unchecked(i + 1) };
                let val = unsafe { offsets.get_unchecked_mut(i) };
                *val = start_offset + next_val;
            }
        }
    } else {
        let compression = Compression::from_codec(read_u8(reader, buf.as_mut_slice())?)?;
        let mut buf = vec![0u8; 4];
        let compressed_size = read_u32(reader, buf.as_mut_slice())? as usize;
        let _uncompressed_size = read_u32(reader, buf.as_mut_slice())? as usize;

        let compressor = compression.create_compressor();
        // already fit in buffer
        let mut use_inner = false;
        reader.fill_buf()?;

        let input = if reader.buffer_bytes().len() >= compressed_size {
            use_inner = true;
            reader.buffer_bytes()
        } else {
            scratch.resize(compressed_size, 0);
            reader.read_exact(scratch.as_mut_slice())?;
            scratch.as_slice()
        };

        compressor.decompress_binary_array(&input[..compressed_size], offsets, values)?;
        if use_inner {
            reader.consume(compressed_size);
        }
    }
    Ok(())
}

fn try_new_binary_array<O: Offset>(
    data_type: DataType,
    offsets: OffsetsBuffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
) -> Result<Box<dyn Array>> {
    if matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) {
        let array = Utf8Array::<O>::try_new(data_type.clone(), offsets, values, validity)?;
        Ok(Box::new(array) as Box<dyn Array>)
    } else {
        let array = BinaryArray::<O>::try_new(data_type.clone(), offsets, values, validity)?;
        Ok(Box::new(array) as Box<dyn Array>)
    }
}
