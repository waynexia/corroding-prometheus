use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray, Int64Array};
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

        Self::new(Arc::new(
            DictionaryArray::try_new(&key_array, &values).unwrap(),
        ))
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
        let array = self.array.values().slice(offset as usize, length as usize);

        Some(array)
    }

    pub fn into_dict(self) -> Arc<DictionaryArray<Int64Type>> {
        self.array
    }
}
