use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray};
use arrow::datatypes::Int64Type;

/// An compound logical "vector"/"array" type. Represent a "matrix" from promql
pub struct Matrix {
    array: Arc<DictionaryArray<Int64Type>>,
}

impl Matrix {
    pub fn new(dict: Arc<DictionaryArray<Int64Type>>) -> Self {
        Self { array: dict }
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
}
