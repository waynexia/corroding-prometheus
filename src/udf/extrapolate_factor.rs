use std::sync::Arc;

use arrow::array::{DictionaryArray, PrimitiveArray};
use arrow::datatypes::{DataType, Int64Type};
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ColumnarValue;

use crate::matrix::Matrix;

struct ExtrapolateFactor {}

impl ExtrapolateFactor {
    fn name() -> &'static str {
        "prom_extrapolate_factor"
    }

    fn input_type() -> DataType {
        DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Int64))
    }

    fn return_type() -> DataType {
        DataType::Int64
    }

    fn calc(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        // construct matrix from input
        assert_eq!(input.len(), 3);
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
        // get range_end and range interval
        let range_end = if let ColumnarValue::Array(array) = &input[1] {
            array
                .as_any()
                .downcast_ref::<Arc<PrimitiveArray<Int64Type>>>()
                .unwrap()
                .clone()
        } else {
            return Err(DataFusionError::Execution(
                "expect array as input, found scalar value".to_string(),
            ));
        };
        let range = if let ColumnarValue::Array(array) = &input[2] {
            array
                .as_any()
                .downcast_ref::<Arc<PrimitiveArray<Int64Type>>>()
                .unwrap()
                .clone()
        } else {
            return Err(DataFusionError::Execution(
                "expect array as input, found scalar value".to_string(),
            ));
        };

        // calculation
        let mut result_array = Vec::with_capacity(matrix.len());
        for index in 0..matrix.len() {
            let ts_array = matrix.get(index).unwrap();
            let ts_array = ts_array
                .as_any()
                .downcast_ref::<Arc<PrimitiveArray<Int64Type>>>()
                .unwrap()
                .values();
            let range_end_array = range_end.values();
            let range_array = range.values();

            // refer to functions.go extrapolatedRate fn
            // assume offset is processed
            // assume `enh.TS` is the first timestamp (TODO: verify this assumption)
            let range_start = range_end_array[index] - range_array[index];
            let range_end = range_end_array[index];
            let duration_to_start = (ts_array.first().unwrap() - range_start) as f64 / 1000.0;
            let duration_to_end = (range_end - ts_array.last().unwrap()) as f64 / 1000.0;
            let sampled_interval =
                (ts_array.last().unwrap() - ts_array.first().unwrap()) as f64 / 1000.0;
            let average_duration_between_samples = sampled_interval / (ts_array.len() - 1) as f64;
            // TODO: functions.go L122 - L134 may require the result of `increase` operator
            let extrapolation_threshold = average_duration_between_samples * 1.1;
            let mut extrapolate_to_interval = sampled_interval;

            if duration_to_start < extrapolation_threshold {
                extrapolate_to_interval += duration_to_start;
            } else {
                extrapolate_to_interval += average_duration_between_samples / 2.0;
            }
            if duration_to_end < extrapolation_threshold {
                extrapolate_to_interval += duration_to_end;
            } else {
                extrapolate_to_interval += average_duration_between_samples / 2.0;
            }
            let factor = extrapolate_to_interval / sampled_interval;

            result_array.push(factor);
        }

        let result = ColumnarValue::Array(Arc::new(PrimitiveArray::from_iter(result_array)));
        Ok(result)
    }
}
