use arrow::types::{i256, NativeType};

use std::hash::Hash;

pub trait IntegerType: NativeType + PartialOrd + Hash + Eq {}

macro_rules! integer_type {
    ($type:ty) => {
        impl IntegerType for $type {}
    };
}

integer_type!(u8);
integer_type!(u16);
integer_type!(u32);
integer_type!(u64);
integer_type!(i8);
integer_type!(i16);
integer_type!(i32);
integer_type!(i64);
integer_type!(i128);
integer_type!(i256);
