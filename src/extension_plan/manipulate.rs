use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use arrow::array::PrimitiveArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimestampMillisecondType};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};

use crate::matrix::Matrix;
use crate::{utils, Millisecond, TIMESTAMP_COLUMN_NAME};

#[derive(Debug)]
pub struct SeriesManipulate {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    range: Millisecond,
    column_to_manipulate: Vec<String>,

    input: LogicalPlan,
    output_schema: SchemaRef,
}

impl SeriesManipulate {
    pub fn new(
        start: SystemTime,
        end: SystemTime,
        interval: Duration,
        range: Duration,
        column_to_manipulate: Vec<String>,
        input: LogicalPlan,
    ) -> Self {
        let output_schema = Self::calculate_output_schema(
            &utils::df_schema_to_arrow_schema(input.schema()),
            &column_to_manipulate,
        );
        Self {
            start: utils::system_time_to_i64(start),
            end: utils::system_time_to_i64(end),
            interval: interval.as_millis() as i64,
            range: range.as_millis() as i64,
            input,
            column_to_manipulate,
            output_schema,
        }
    }

    pub fn calculate_output_schema(
        input_schema: &Schema,
        column_to_manipulate: &[String],
    ) -> SchemaRef {
        let manipulate_set = column_to_manipulate.iter().collect::<HashSet<_>>();
        let new_fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|field| {
                if manipulate_set.contains(field.name()) {
                    Field::new(
                        field.name(),
                        DataType::Dictionary(
                            Box::new(Matrix::key_type()),
                            Box::new(field.data_type().clone()),
                        ),
                        field.is_nullable(),
                    )
                } else {
                    field.clone()
                }
            })
            .collect();
        Arc::new(Schema::new(new_fields))
    }
}

impl UserDefinedLogicalNode for SeriesManipulate {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        todo!()
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        todo!()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        todo!()
    }

    fn from_template(
        &self,
        exprs: &[datafusion::logical_expr::Expr],
        inputs: &[LogicalPlan],
    ) -> std::sync::Arc<dyn UserDefinedLogicalNode> {
        todo!()
    }
}

#[derive(Debug)]
pub struct SeriesManipulateExec {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    range: Millisecond,
    column_to_manipulate: Vec<String>,

    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
}

impl SeriesManipulateExec {
    pub fn new(
        start: Millisecond,
        end: Millisecond,
        interval: Millisecond,
        range: Millisecond,
        column_to_manipulate: Vec<String>,

        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            start,
            end,
            interval,
            range,
            column_to_manipulate,
            input,
            output_schema,
        }
    }
}

impl ExecutionPlan for SeriesManipulateExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.output_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion::common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(SeriesManipulateStream::new(
            self.start,
            self.end,
            self.interval,
            self.range,
            self.column_to_manipulate.clone(),
            input,
            self.output_schema.clone(),
        )))
    }

    fn statistics(&self) -> datafusion::common::Statistics {
        todo!()
    }
}

pub struct SeriesManipulateStream {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    range: Millisecond,
    column_to_manipulate: Vec<String>,

    input_schema: SchemaRef,
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,
}

impl SeriesManipulateStream {
    fn new(
        start: Millisecond,
        end: Millisecond,
        interval: Millisecond,
        range: Millisecond,
        column_to_manipulate: Vec<String>,
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            start,
            end,
            interval,
            range,
            column_to_manipulate,
            input_schema: input.schema(),
            input,
            output_schema,
        }
    }

    fn manipulate(&self, input: RecordBatch) -> ArrowResult<RecordBatch> {
        let column_indices_to_manipulate = self
            .column_to_manipulate
            .iter()
            .map(|col| {
                self.input_schema
                    .column_with_name(col)
                    .ok_or_else(|| {
                        ArrowError::SchemaError(format!(
                            "field {:?} not found in input schema",
                            col
                        ))
                    })
                    .map(|tuple| tuple.0)
            })
            .collect::<ArrowResult<Vec<_>>>()?;

        // calculate the range
        let ranges = self.calculate_range(&input);
        println!("Manipulate ranges: {:?}", ranges);

        // transform columns
        let mut new_columns = input.columns().to_vec();
        for index in column_indices_to_manipulate {
            let column = input.column(index);
            let new_column = Matrix::from_raw(column.clone(), &ranges).into_dict();
            new_columns[index] = new_column;
        }

        RecordBatch::try_new(self.output_schema.clone(), new_columns)
    }

    fn calculate_range(&self, input: &RecordBatch) -> Vec<(i32, i32)> {
        let ts_column_idx = self
            .input_schema
            .column_with_name(TIMESTAMP_COLUMN_NAME)
            .expect("timestamp column not found")
            .0;
        let ts_column = input
            .column(ts_column_idx)
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
            .unwrap();

        let mut result = vec![];

        for curr_ts in (self.start..=self.end).step_by(self.interval as _) {
            let mut range_start = ts_column.len();
            let mut range_end = 0;
            for (index, ts) in ts_column.values().iter().enumerate() {
                if ts + self.range >= curr_ts {
                    range_start = range_start.min(index);
                }
                if *ts <= curr_ts {
                    range_end = range_end.max(index);
                }
            }
            result.push((range_start as _, (range_end - range_start + 1) as _));
        }

        result
    }
}

impl RecordBatchStream for SeriesManipulateStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for SeriesManipulateStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(batch) => {
                Poll::Ready(batch.map(|batch| batch.map(|batch| self.manipulate(batch)).flatten()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
