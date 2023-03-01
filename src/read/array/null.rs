use crate::read::PageIterator;
use crate::PageMeta;
use arrow::{
    array::{Array, NullArray},
    datatypes::DataType,
    error::Result,
};

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

impl<I> NullIter<I>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
{
    fn deserialize(&mut self, num_values: u64) -> Result<Box<dyn Array>> {
        let length = num_values as usize;
        let array = NullArray::try_new(self.data_type.clone(), length)?;
        Ok(Box::new(array) as Box<dyn Array>)
    }
}

impl<I> Iterator for NullIter<I>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
{
    type Item = Result<Box<dyn Array>>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self.iter.nth(n) {
            Some(Ok((num_values, mut buffer))) => {
                self.iter.swap_buffer(&mut buffer);
                Some(self.deserialize(num_values))
            }
            Some(Err(err)) => Some(Result::Err(err)),
            None => None,
        }
    }

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((num_values, mut buffer))) => {
                self.iter.swap_buffer(&mut buffer);
                Some(self.deserialize(num_values))
            }
            Some(Err(err)) => Some(Result::Err(err)),
            None => None,
        }
    }
}

pub fn read_null(data_type: DataType, page_metas: Vec<PageMeta>) -> Result<Box<dyn Array>> {
    let length = page_metas.iter().map(|p| p.num_values as usize).sum();

    let array = NullArray::try_new(data_type, length)?;
    Ok(Box::new(array) as Box<dyn Array>)
}
