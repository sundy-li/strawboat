use arrow::types::{i256, NativeType};

use std::hash::Hash;

pub trait IntegerType: NativeType + PartialOrd + Hash + Eq {
    fn as_i64(&self) -> i64;
    fn sub(&self, other: &Self) -> Self;
    fn add(&self, other: &Self) -> Self;
}

macro_rules! integer_type {
    ($type:ty) => {
        impl IntegerType for $type {
            fn as_i64(&self) -> i64 {
                *self as i64
            }

            fn sub(&self, other: &Self) -> Self {
                self - other
            }

            fn add(&self, other: &Self) -> Self {
                self + other
            }
        }
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
// integer_type!(days_ms);
// integer_type!(months_days_ns);

impl IntegerType for i128 {
    fn as_i64(&self) -> i64 {
        *self as i64
    }

    fn sub(&self, other: &Self) -> Self {
        self - other
    }

    fn add(&self, other: &Self) -> Self {
        self + other
    }
}

impl IntegerType for i256 {
    fn as_i64(&self) -> i64 {
        self.0.as_i64()
    }

    fn sub(&self, _other: &Self) -> Self {
        unimplemented!()
    }

    fn add(&self, _other: &Self) -> Self {
        unimplemented!()
    }
}
