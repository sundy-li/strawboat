use arrow::array::{Array, StructArray};
use arrow::datatypes::{DataType, Field};
use arrow::error::Result;
use arrow::io::parquet::read::NestedState;

use crate::read::deserialize::NestedIters;

type StructValues = Vec<Option<Result<(NestedState, Box<dyn Array>)>>>;

/// An iterator adapter over [`DynIter`] assumed to be encoded as Struct arrays
pub struct StructIterator<'a> {
    iters: Vec<NestedIters<'a>>,
    fields: Vec<Field>,
}

impl<'a> StructIterator<'a> {
    /// Creates a new [`StructIterator`] with `iters` and `fields`.
    pub fn new(iters: Vec<NestedIters<'a>>, fields: Vec<Field>) -> Self {
        assert_eq!(iters.len(), fields.len());
        Self { iters, fields }
    }
}

impl<'a> StructIterator<'a> {
    fn deserialize(
        &mut self,
        values: StructValues,
    ) -> Option<Result<(NestedState, Box<dyn Array>)>> {
        // This code is copied from arrow2 `StructIterator` and adds a custom `nth` method implementation
        // https://github.com/jorgecarleitao/arrow2/blob/main/src/io/parquet/read/deserialize/struct_.rs
        if values.iter().any(|x| x.is_none()) {
            return None;
        }

        // todo: unzip of Result not yet supportted in stable Rust
        let mut nested = vec![];
        let mut new_values = vec![];
        for x in values {
            match x.unwrap() {
                Ok((nest, values)) => {
                    new_values.push(values);
                    nested.push(nest);
                }
                Err(e) => return Some(Err(e)),
            }
        }
        let mut nested = nested.pop().unwrap();
        let (_, validity) = nested.nested.pop().unwrap().inner();
        Some(Ok((
            nested,
            Box::new(StructArray::new(
                DataType::Struct(self.fields.clone()),
                new_values,
                validity.and_then(|x| x.into()),
            )),
        )))
    }
}

impl<'a> Iterator for StructIterator<'a> {
    type Item = Result<(NestedState, Box<dyn Array>)>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let values = self
            .iters
            .iter_mut()
            .map(|iter| iter.nth(n))
            .collect::<Vec<_>>();

        self.deserialize(values)
    }

    fn next(&mut self) -> Option<Self::Item> {
        let values = self
            .iters
            .iter_mut()
            .map(|iter| iter.next())
            .collect::<Vec<_>>();

        self.deserialize(values)
    }
}
