use crate::with_match_primitive_type;
use arrow::array::*;
use arrow::datatypes::{DataType, PhysicalType};
use arrow::error::Result;
use arrow::offset::OffsetsBuffer;

use super::{array::*, NativeReadBuf};

pub fn read<R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<Box<dyn Array>> {
    use PhysicalType::*;

    match data_type.to_physical_type() {
        Null => read_null(data_type, length).map(|x| x.boxed()),
        Boolean => read_boolean(reader, data_type, length, scratch).map(|x| x.boxed()),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            read_primitive::<$T, _>(
                reader,
                data_type,
                length,
                scratch
            )
            .map(|x| x.boxed())
        }),
        Binary => read_binary::<i32, _>(reader, data_type, length, scratch).map(|x| x.boxed()),
        LargeBinary => read_binary::<i64, _>(reader, data_type, length, scratch).map(|x| x.boxed()),

        FixedSizeBinary => unimplemented!(),

        Utf8 => read_utf8::<i32, _>(reader, data_type, length, scratch).map(|x| x.boxed()),

        LargeUtf8 => read_utf8::<i64, _>(reader, data_type, length, scratch).map(|x| x.boxed()),
        List => {
            let sub_filed = if let DataType::List(field) = data_type.to_logical_type() {
                field
            } else {
                unreachable!()
            };
            let offset_size =
                read_primitive::<i32, _>(reader, DataType::Int32, 1, scratch)?.value(0);
            let offset =
                read_primitive::<i32, _>(reader, DataType::Int32, offset_size as usize, scratch)?;
            let value = read(
                reader,
                sub_filed.clone().data_type,
                length * (offset_size - 1) as usize,
                scratch,
            )?;
            ListArray::try_new(
                data_type,
                unsafe { OffsetsBuffer::new_unchecked(offset.values().to_owned()) },
                value,
                None,
            )
            .map(|x| x.boxed())
        }
        LargeList => unimplemented!(),
        FixedSizeList => unimplemented!(),
        Struct => {
            let children_fields = if let DataType::Struct(children) = data_type.to_logical_type() {
                children
            } else {
                unreachable!()
            };
            let mut value = vec![];
            for f in children_fields {
                value.push(read(reader, f.clone().data_type, length, scratch)?);
            }
            StructArray::try_new(data_type, value, None).map(|x| x.boxed())
        }
        Dictionary(_key_type) => unimplemented!(),
        Union => unimplemented!(),
        Map => unimplemented!(),
    }
}
