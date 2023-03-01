use super::{array::*, NativeReadBuf};
use crate::{with_match_primitive_type, PageMeta};
use arrow::array::*;
use arrow::compute::concatenate::concatenate;
use arrow::datatypes::{DataType, Field, PhysicalType};
use arrow::error::Result;
use arrow::io::parquet::read::{create_list, create_map, n_columns, InitNested, NestedState};
use parquet2::metadata::ColumnDescriptor;

pub fn read_simple<R: NativeReadBuf>(
    reader: &mut R,
    field: Field,
    page_metas: Vec<PageMeta>,
) -> Result<Box<dyn Array>> {
    use PhysicalType::*;

    let is_nullable = field.is_nullable;
    let data_type = field.data_type().clone();

    match data_type.to_physical_type() {
        Null => read_null(data_type, page_metas),
        Boolean => read_boolean(reader, is_nullable, data_type, page_metas),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            read_primitive::<$T, _>(
                reader,
                is_nullable,
                data_type,
                page_metas,
            )
        }),
        Binary => read_binary::<i32, _>(reader, is_nullable, data_type, page_metas),
        LargeBinary => read_binary::<i64, _>(reader, is_nullable, data_type, page_metas),
        FixedSizeBinary => unimplemented!(),
        Utf8 => read_utf8::<i32, _>(reader, is_nullable, data_type, page_metas),
        LargeUtf8 => read_utf8::<i64, _>(reader, is_nullable, data_type, page_metas),
        _ => unreachable!(),
    }
}

pub fn read_nested<R: NativeReadBuf>(
    mut readers: Vec<R>,
    field: Field,
    mut leaves: Vec<ColumnDescriptor>,
    mut init: Vec<InitNested>,
    mut page_metas: Vec<Vec<PageMeta>>,
) -> Result<Vec<(NestedState, Box<dyn Array>)>> {
    use PhysicalType::*;

    Ok(match field.data_type().to_physical_type() {
        Null => unimplemented!(),
        Boolean => {
            init.push(InitNested::Primitive(field.is_nullable));
            read_nested_boolean(
                &mut readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            init.push(InitNested::Primitive(field.is_nullable));
            read_nested_primitive::<$T, _>(
                &mut readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
                page_metas.pop().unwrap(),
            )?
        }),
        Binary => {
            init.push(InitNested::Primitive(field.is_nullable));
            read_nested_binary::<i32, _>(
                &mut readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        LargeBinary => {
            init.push(InitNested::Primitive(field.is_nullable));
            read_nested_binary::<i64, _>(
                &mut readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        Utf8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            read_nested_utf8::<i32, _>(
                &mut readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        LargeUtf8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            read_nested_utf8::<i64, _>(
                &mut readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        FixedSizeBinary => unimplemented!(),
        _ => match field.data_type().to_logical_type() {
            DataType::List(inner)
            | DataType::LargeList(inner)
            | DataType::FixedSizeList(inner, _) => {
                init.push(InitNested::List(field.is_nullable));
                let results =
                    read_nested(readers, inner.as_ref().clone(), leaves, init, page_metas)?;
                let mut arrays = Vec::with_capacity(results.len());
                for (mut nested, values) in results {
                    let _ = nested.nested.pop().unwrap();
                    let array = create_list(field.data_type().clone(), &mut nested, values);
                    arrays.push((nested, array));
                }
                arrays
            }
            DataType::Map(inner, _) => {
                init.push(InitNested::List(field.is_nullable));
                let results =
                    read_nested(readers, inner.as_ref().clone(), leaves, init, page_metas)?;
                let mut arrays = Vec::with_capacity(results.len());
                for (mut nested, values) in results {
                    let _ = nested.nested.pop().unwrap();
                    let array = create_map(field.data_type().clone(), &mut nested, values);
                    arrays.push((nested, array));
                }
                arrays
            }
            DataType::Struct(fields) => {
                let mut results = fields
                    .iter()
                    .map(|f| {
                        let mut init = init.clone();
                        init.push(InitNested::Struct(field.is_nullable));
                        let n = n_columns(&f.data_type);
                        let readers = readers.drain(..n).collect();
                        let leaves = leaves.drain(..n).collect();
                        let page_metas = page_metas.drain(..n).collect();
                        read_nested(readers, f.clone(), leaves, init, page_metas)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut arrays = Vec::with_capacity(results[0].len());
                while !results[0].is_empty() {
                    let mut nesteds = Vec::with_capacity(fields.len());
                    let mut values = Vec::with_capacity(fields.len());
                    for result in results.iter_mut() {
                        let (nested, value) = result.pop().unwrap();
                        nesteds.push(nested);
                        values.push(value);
                    }
                    let array = create_struct(fields.clone(), &mut nesteds, values);
                    arrays.push(array);
                }
                arrays.reverse();
                arrays
            }
            _ => unreachable!(),
        },
    })
}

/// Read all pages of column at once.
pub fn batch_read_array<R: NativeReadBuf>(
    mut readers: Vec<R>,
    leaves: Vec<ColumnDescriptor>,
    field: Field,
    is_nested: bool,
    mut page_metas: Vec<Vec<PageMeta>>,
) -> Result<Box<dyn Array>> {
    if is_nested {
        let results = read_nested(readers, field, leaves, vec![], page_metas)?;
        let arrays: Vec<&dyn Array> = results.iter().map(|(_, v)| v.as_ref()).collect();
        let array = concatenate(&arrays).unwrap();
        Ok(array)
    } else {
        read_simple(
            &mut readers.pop().unwrap(),
            field,
            page_metas.pop().unwrap(),
        )
    }
}
