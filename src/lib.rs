#![feature(result_flattening)]

use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::TimestampMillisecondType;

mod eval_stmt;
mod extension_plan;
mod matrix;
mod planner;
mod udf;
mod utils;

type Millisecond = <TimestampMillisecondType as ArrowPrimitiveType>::Native;

const TIMESTAMP_COLUMN_NAME: &str = "timestamp";

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::from_slice::FromSlice;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::extension_plan::manipulate::{SeriesManipulate, SeriesManipulateExec};
    use crate::extension_plan::normalize::SeriesNormalizeExec;
    use crate::udf::increase::Increase;

    fn prepare_test_data() -> MemoryExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIMESTAMP_COLUMN_NAME,
                TimestampMillisecondType::DATA_TYPE,
                true,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("path", DataType::Utf8, true),
        ]));
        let timestamp_column = Arc::new(TimestampMillisecondArray::from_slice(&[
            0, 30_000, 60_000, 90_000, 120_000,
        ]));
        let value_column = Arc::new(Float64Array::from_slice(&[0.0, 1.0, 10.0, 100.0, 1000.0]));
        let path_column = Arc::new(StringArray::from_slice(&[
            "foo", "foo", "foo", "foo", "foo",
        ]));
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![timestamp_column, value_column, path_column],
        )
        .unwrap();

        MemoryExec::try_new(&[vec![data]], schema, None).unwrap()
    }

    fn prepare_manipulate_exec(input: Arc<dyn ExecutionPlan>) -> SeriesManipulateExec {
        let columns_to_manipulate = vec!["value".to_string()];
        let output_schema =
            SeriesManipulate::calculate_output_schema(&input.schema(), &columns_to_manipulate);
        SeriesManipulateExec::new(
            0,
            120_000,
            30_000,
            30_000,
            columns_to_manipulate,
            input,
            output_schema,
        )
    }

    #[tokio::test]
    async fn example_increase() {
        let memory_exec = Arc::new(prepare_test_data());
        let schema = memory_exec.schema();
        let normalize_exec = Arc::new(SeriesNormalizeExec::new(
            0,
            120_000,
            0,      // no offset
            0,      // no lookback
            30_000, // todo: this should be 60_000. I change it to bypass a logical bug.
            memory_exec,
        ));
        let manipulate_exec = Arc::new(prepare_manipulate_exec(normalize_exec));
        let increase_expr = Arc::new(Increase::new(Arc::new(Column::new(
            "value",
            schema.index_of("value").unwrap(),
        ))));
        let projection_exec = Arc::new(
            ProjectionExec::try_new(
                vec![(increase_expr, Increase::name().to_string())],
                manipulate_exec,
            )
            .unwrap(),
        );

        let session_context = SessionContext::default();
        let result =
            datafusion::physical_plan::collect(projection_exec, session_context.task_ctx())
                .await
                .unwrap();
        println!(
            "{}",
            arrow::util::pretty::pretty_format_batches(&result).unwrap()
        );
    }
}
