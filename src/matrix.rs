use std::sync::Arc;

use arrow::array::{Array, ArrayData, ArrayRef, DictionaryArray, Int64Array};
use arrow::datatypes::{DataType, Int64Type};

/// An compound logical "vector"/"array" type. Represent a "matrix" from promql
pub struct Matrix {
    array: Arc<DictionaryArray<Int64Type>>,
}

impl Matrix {
    pub const fn key_type() -> DataType {
        DataType::Int64
    }

    pub fn new(dict: Arc<DictionaryArray<Int64Type>>) -> Self {
        Self { array: dict }
    }

    pub fn from_raw(values: ArrayRef, ranges: &[(i32, i32)]) -> Self {
        let key_array = Int64Array::from_iter(
            ranges
                .into_iter()
                .map(|(offset, length)| bytemuck::cast::<[i32; 2], i64>([*offset, *length])),
        );

        // Build from ArrayData to bypass the "offset" checker. Because
        // we are not using "keys" as-is.
        // This paragraph is copied from arrow-rs dictionary_array.rs `try_new()`.
        let mut data = ArrayData::builder(DataType::Dictionary(
            Box::new(Self::key_type()),
            Box::new(values.data_type().clone()),
        ))
        .len(key_array.len())
        .add_buffer(key_array.data().buffers()[0].clone())
        .add_child_data(values.data().clone());
        match key_array.data().null_buffer() {
            Some(buffer) if key_array.data().null_count() > 0 => {
                data = data
                    .null_bit_buffer(Some(buffer.clone()))
                    .null_count(key_array.data().null_count());
            }
            _ => data = data.null_count(0),
        }
        let array = unsafe { data.build_unchecked() };

        Self::new(Arc::new(array.into()))
    }

    pub fn len(&self) -> usize {
        self.array.keys().len()
    }

    pub fn get(&self, index: usize) -> Option<ArrayRef> {
        if index >= self.len() {
            return None;
        }

        let compound_key = self.array.keys().value(index);
        let [offset, length] = bytemuck::cast::<i64, [i32; 2]>(compound_key);
        println!("offset: {}, length: {}", offset, length);
        let array = self.array.values().slice(offset as usize, length as usize);

        Some(array)
    }

    pub fn into_dict(self) -> Arc<DictionaryArray<Int64Type>> {
        self.array
    }
}
