use std::sync::Arc;

use arrow::array::{DictionaryArray, PrimitiveArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ColumnarValue;

use crate::matrix::Matrix;

struct Increase {}

impl Increase {
    fn name() -> &'static str {
        "prom_increase"
    }

    fn input_type() -> DataType {
        DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Float64))
    }

    fn return_type() -> DataType {
        DataType::Float64
    }

    fn calc(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        // construct matrix from input
        assert_eq!(input.len(), 1);
        let input_array = if let ColumnarValue::Array(array) = input.first().unwrap() {
            array
        } else {
            return Err(DataFusionError::Execution(
                "expect array as input, found scalar value".to_string(),
            ));
        };
        let dict_array = input_array
            .as_any()
            .downcast_ref::<Arc<DictionaryArray<Int64Type>>>()
            .unwrap();
        let matrix = Matrix::new(dict_array.clone());

        // calculation
        let mut result_array = Vec::with_capacity(matrix.len());
        for index in 0..matrix.len() {
            let range = matrix.get(index).unwrap();
            let range = range
                .as_any()
                .downcast_ref::<Arc<PrimitiveArray<Float64Type>>>()
                .unwrap()
                .values();

            // refer to functions.go L83-L110
            let mut result_value = range.last().unwrap() - range.first().unwrap();
            for window in range.windows(2) {
                let prev = window[0];
                let curr = window[1];
                if curr < prev {
                    result_value += prev
                }
            }

            result_array.push(result_value);
        }

        let result = ColumnarValue::Array(Arc::new(PrimitiveArray::from_iter(result_array)));
        Ok(result)
    }
}
