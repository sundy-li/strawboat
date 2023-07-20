use arrow::types::NativeType;
use num::Float;
use ordered_float::OrderedFloat;

use std::hash::Hash;

pub trait DoubleType: Copy + Clone + NativeType + Float {
    type OrderType: std::fmt::Debug
        + std::fmt::Display
        + Eq
        + Hash
        + PartialOrd
        + Hash
        + Eq
        + Copy
        + Clone;

    fn as_order(&self) -> Self::OrderType;

    fn from_order(order: Self::OrderType) -> Self;
}

macro_rules! double_type {
    ($type:ty, $order_type: ty) => {
        impl DoubleType for $type {
            type OrderType = $order_type;

            fn as_order(&self) -> Self::OrderType {
                OrderedFloat(*self)
            }

            fn from_order(order: Self::OrderType) -> Self {
                order.0
            }
        }
    };
}

type F32 = OrderedFloat<f32>;
type F64 = OrderedFloat<f64>;

double_type!(f32, F32);
double_type!(f64, F64);
