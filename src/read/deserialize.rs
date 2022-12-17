use crate::read::Compression;
use crate::with_match_primitive_type;
use arrow::array::*;
use arrow::datatypes::{DataType, PhysicalType};
use arrow::error::Result;

use super::{array::*, PaReadBuf};

pub fn read<R: PaReadBuf>(
    reader: &mut R,
    data_type: DataType,
    is_little_endian: bool,
    compression: Option<Compression>,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<Box<dyn Array>> {
    use PhysicalType::*;

    match data_type.to_physical_type() {
        Null => read_null(data_type, length).map(|x| x.boxed()),
        Boolean => read_boolean(
            reader,
            data_type,
            is_little_endian,
            compression,
            length,
            scratch,
        )
        .map(|x| x.boxed()),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            read_primitive::<$T, _>(
                reader,
                data_type,
                is_little_endian,
                compression,
                length,
                scratch
            )
            .map(|x| x.boxed())
        }),
        Binary => read_binary::<i32, _>(
            reader,
            data_type,
            is_little_endian,
            compression,
            length,
            scratch,
        )
        .map(|x| x.boxed()),
        LargeBinary => read_binary::<i64, _>(
            reader,
            data_type,
            is_little_endian,
            compression,
            length,
            scratch,
        )
        .map(|x| x.boxed()),

        FixedSizeBinary => unimplemented!(),

        Utf8 => read_utf8::<i32, _>(
            reader,
            data_type,
            is_little_endian,
            compression,
            length,
            scratch,
        )
        .map(|x| x.boxed()),

        LargeUtf8 => read_utf8::<i64, _>(
            reader,
            data_type,
            is_little_endian,
            compression,
            length,
            scratch,
        )
        .map(|x| x.boxed()),

        List => unimplemented!(),
        LargeList => unimplemented!(),
        FixedSizeList => unimplemented!(),
        Struct => unimplemented!(),
        Dictionary(_key_type) => unimplemented!(),
        Union => unimplemented!(),
        Map => unimplemented!(),
    }
}
