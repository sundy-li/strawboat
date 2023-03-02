use arrow::array::Array;
use arrow::datatypes::{DataType, Field};
use arrow::error::Result;
use arrow::io::parquet::read::{create_list, NestedState};

use crate::read::deserialize::DynIter;
use crate::read::reader::is_primitive_or_struct;

/// An iterator adapter over [`DynIter`] assumed to be encoded as List arrays
pub struct ListIterator<'a> {
    iter: DynIter<'a, Result<(NestedState, Box<dyn Array>)>>,
    field: Field,
}

impl<'a> ListIterator<'a> {
    /// Creates a new [`ListIterator`] with `iter` and `field`.
    pub fn new(iter: DynIter<'a, Result<(NestedState, Box<dyn Array>)>>, field: Field) -> Self {
        Self { iter, field }
    }
}

impl<'a> ListIterator<'a> {
    fn deserialize(
        &mut self,
        value: Option<Result<(NestedState, Box<dyn Array>)>>,
    ) -> Option<Result<(NestedState, Box<dyn Array>)>> {
        let (mut nested, values) = match value {
            Some(Ok((nested, values))) => (nested, values),
            Some(Err(err)) => return Some(Err(err)),
            None => return None,
        };
        match &self.field.data_type {
            DataType::List(inner)
            | DataType::LargeList(inner)
            | DataType::FixedSizeList(inner, _) => {
                if is_primitive_or_struct(&inner.data_type) {
                    // pop the primitive nested
                    let _ = nested.nested.pop().unwrap();
                }
            }
            _ => unreachable!(),
        }
        let array = create_list(self.field.data_type().clone(), &mut nested, values);
        Some(Ok((nested, array)))
    }
}

impl<'a> Iterator for ListIterator<'a> {
    type Item = Result<(NestedState, Box<dyn Array>)>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let value = self.iter.nth(n);
        self.deserialize(value)
    }

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.iter.next();
        self.deserialize(value)
    }
}
