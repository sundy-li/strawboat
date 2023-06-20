use arrow::datatypes::DataType;

struct Bitcast {
    data_type: DataType,
}

impl Bitcast {
    fn compress_array(
        &self,
        _array: &dyn arrow::array::Array,
        _output_buf: &mut Vec<u8>,
    ) -> arrow::error::Result<usize> {
        todo!()
    }

    fn decompress_array(
        &self,
        _input: &[u8],
        _array: &mut dyn arrow::array::MutableArray,
    ) -> arrow::error::Result<()> {
        todo!()
    }

    fn support_datatype(&self, _data_type: &DataType) -> bool {
        todo!()
    }
}
