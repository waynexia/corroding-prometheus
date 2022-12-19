use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, Float64Builder, Int64Builder, PrimitiveArray, StringArray, StringBuilder,
};
use arrow::datatypes::{Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;

pub fn system_time_to_i64(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).map_or_else(
        |e| -(e.duration().as_millis() as i64),
        |d| d.as_millis() as i64,
    )
}

pub fn take_record_batch_optional(
    record_batch: RecordBatch,
    indices: Vec<Option<usize>>,
) -> RecordBatch {
    let arrays = record_batch
        .columns()
        .iter()
        .map(|array| take_array_optional(array, &indices))
        .collect();

    RecordBatch::try_new(record_batch.schema(), arrays).unwrap()
}

pub fn take_array_optional(array: &Arc<dyn Array>, indices: &[Option<usize>]) -> Arc<dyn Array> {
    match array.data_type() {
        arrow::datatypes::DataType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<Arc<PrimitiveArray<Int64Type>>>()
                .unwrap();
            let mut builder = Int64Builder::with_capacity(indices.len());
            for index in indices {
                if let Some(i) = index {
                    builder.append_value(array.value(*i));
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish()) as _
        }
        arrow::datatypes::DataType::Float64 => {
            let array = array
                .as_any()
                .downcast_ref::<Arc<PrimitiveArray<Float64Type>>>()
                .unwrap();
            let mut builder = Float64Builder::with_capacity(indices.len());
            for index in indices {
                if let Some(i) = index {
                    builder.append_value(array.value(*i));
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish()) as _
        }
        arrow::datatypes::DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<Arc<StringArray>>().unwrap();
            let mut builder = StringBuilder::with_capacity(indices.len(), indices.len());
            for index in indices {
                if let Some(i) = index {
                    builder.append_value(array.value(*i));
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish()) as _
        }
        _ => unreachable!(),
    }
}
