use super::{array::*, PageIterator};
use crate::with_match_primitive_type;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, PhysicalType};
use arrow::error::Result;
use arrow::io::parquet::read::{
    create_list, n_columns, ArrayIter, InitNested, NestedArrayIter, NestedState, StructIterator,
};
use parquet2::metadata::ColumnDescriptor;

#[inline]
fn dyn_iter<'a, A, I>(iter: I) -> ArrayIter<'a>
where
    A: Array,
    I: Iterator<Item = Result<A>> + Send + Sync + 'a,
{
    Box::new(iter.map(|x| x.map(|x| Box::new(x) as Box<dyn Array>)))
}

#[inline]
fn nested_dyn_iter<'a, A, I>(iter: I) -> NestedArrayIter<'a>
where
    A: Array,
    I: Iterator<Item = Result<(NestedState, A)>> + Send + Sync + 'a,
{
    Box::new(iter.map(|x| {
        x.map(|(mut nested, array)| {
            let _ = nested.nested.pop().unwrap(); // the primitive
            (nested, Box::new(array) as _)
        })
    }))
}

fn deserialize_simple<'a, I: 'a>(reader: I, field: Field) -> Result<ArrayIter<'a>>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
{
    use PhysicalType::*;

    let is_nullable = field.is_nullable;
    let data_type = field.data_type().clone();

    Ok(match data_type.to_physical_type() {
        Null => dyn_iter(NullIter::new(reader, data_type)),
        Boolean => dyn_iter(BooleanIter::new(reader, is_nullable, data_type)),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            dyn_iter(PrimitiveIter::<_, $T>::new(
                reader,
                is_nullable,
                data_type,
            ))
        }),
        Binary => dyn_iter(BinaryIter::<_, i32>::new(reader, is_nullable, data_type)),
        LargeBinary => dyn_iter(BinaryIter::<_, i64>::new(reader, is_nullable, data_type)),
        Utf8 => dyn_iter(Utf8Iter::<_, i32>::new(reader, is_nullable, data_type)),
        LargeUtf8 => dyn_iter(Utf8Iter::<_, i64>::new(reader, is_nullable, data_type)),
        FixedSizeBinary => unimplemented!(),
        _ => unreachable!(),
    })
}

fn deserialize_nested<'a, I: 'a>(
    mut readers: Vec<I>,
    mut leaves: Vec<ColumnDescriptor>,
    field: Field,
    mut init: Vec<InitNested>,
) -> Result<NestedArrayIter<'a>>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
{
    use PhysicalType::*;

    Ok(match field.data_type().to_physical_type() {
        Null => unimplemented!(),
        Boolean => {
            init.push(InitNested::Primitive(field.is_nullable));
            nested_dyn_iter(BooleanNestedIter::new(
                readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
            ))
        }
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            init.push(InitNested::Primitive(field.is_nullable));
            nested_dyn_iter(PrimitiveNestedIter::<_, $T>::new(
                readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
            ))
        }),
        Binary => {
            init.push(InitNested::Primitive(field.is_nullable));
            nested_dyn_iter(BinaryNestedIter::<_, i32>::new(
                readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
            ))
        }
        LargeBinary => {
            init.push(InitNested::Primitive(field.is_nullable));
            nested_dyn_iter(BinaryNestedIter::<_, i64>::new(
                readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
            ))
        }
        Utf8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            nested_dyn_iter(Utf8NestedIter::<_, i32>::new(
                readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
            ))
        }
        LargeUtf8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            nested_dyn_iter(Utf8NestedIter::<_, i64>::new(
                readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
            ))
        }
        FixedSizeBinary => unimplemented!(),
        _ => match field.data_type().to_logical_type() {
            DataType::List(inner)
            | DataType::LargeList(inner)
            | DataType::FixedSizeList(inner, _) => {
                init.push(InitNested::List(field.is_nullable));
                let iter = deserialize_nested(readers, leaves, inner.as_ref().clone(), init)?;
                let iter = iter.map(move |x| {
                    let (mut nested, array) = x?;
                    let array = create_list(field.data_type().clone(), &mut nested, array);
                    Ok((nested, array))
                });
                Box::new(iter) as _
            }
            DataType::Struct(fields) => {
                let columns = fields
                    .iter()
                    .rev()
                    .map(|f| {
                        let mut init = init.clone();
                        init.push(InitNested::Struct(field.is_nullable));
                        let n = n_columns(&f.data_type);
                        let readers = readers.drain(readers.len() - n..).collect();
                        let leaves = leaves.drain(leaves.len() - n..).collect();
                        deserialize_nested(readers, leaves, f.clone(), init)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let columns = columns.into_iter().rev().collect();
                Box::new(StructIterator::new(columns, fields.clone()))
            }
            _ => unreachable!(),
        },
    })
}

pub fn column_iter_to_arrays<'a, I: 'a>(
    mut readers: Vec<I>,
    leaves: Vec<ColumnDescriptor>,
    field: Field,
    is_nested: bool,
) -> Result<ArrayIter<'a>>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
{
    if is_nested {
        Ok(Box::new(
            deserialize_nested(readers, leaves, field, vec![])?.map(|x| x.map(|x| x.1)),
        ))
    } else {
        deserialize_simple(readers.pop().unwrap(), field)
    }
}
