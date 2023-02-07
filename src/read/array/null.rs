use crate::read::PageIterator;
use arrow::{array::NullArray, datatypes::DataType, error::Result};

#[derive(Debug)]
pub struct NullIter<I>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
{
    iter: I,
    data_type: DataType,
}

impl<I> NullIter<I>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
{
    pub fn new(iter: I, data_type: DataType) -> Self {
        Self { iter, data_type }
    }
}

impl<I> Iterator for NullIter<I>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
{
    type Item = Result<NullArray>;

    fn next(&mut self) -> Option<Self::Item> {
        let (num_values, mut buffer) = match self.iter.next() {
            Some(Ok((num_values, buffer))) => (num_values, buffer),
            Some(Err(err)) => {
                return Some(Result::Err(err));
            }
            None => {
                return None;
            }
        };

        self.iter.swap_buffer(&mut buffer);
        let length = num_values as usize;
        Some(NullArray::try_new(self.data_type.clone(), length))
    }
}
