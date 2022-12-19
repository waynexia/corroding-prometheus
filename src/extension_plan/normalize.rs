use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use arrow::array::{ArrowPrimitiveType, PrimitiveArray, TimestampMillisecondArray};
use arrow::datatypes::{SchemaRef, TimestampMillisecondType};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};

use crate::utils;

type Millisecond = <TimestampMillisecondType as ArrowPrimitiveType>::Native;

const TIMESTAMP_COLUMN_NAME: &str = "timestamp";

#[derive(Debug)]
pub struct SeriesNormalize {
    start: Millisecond,
    end: Millisecond,
    offset: Millisecond,
    lookback_delta: Millisecond,
    interval: Millisecond,

    input: LogicalPlan,
}

impl UserDefinedLogicalNode for SeriesNormalize {
    fn as_any(&self) -> &dyn Any {
        self as _
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
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

impl SeriesNormalize {
    pub fn new(
        start: SystemTime,
        end: SystemTime,
        offset: Duration,
        lookback_delta: Duration,
        interval: Duration,
        input: LogicalPlan,
    ) -> Self {
        Self {
            start: utils::system_time_to_i64(start),
            end: utils::system_time_to_i64(end),
            offset: offset.as_millis() as i64,
            lookback_delta: lookback_delta.as_millis() as i64,
            interval: interval.as_millis() as i64,
            input,
        }
    }
}

#[derive(Debug)]
pub struct SeriesNormalizeExec {
    start: Millisecond,
    end: Millisecond,
    offset: Millisecond,
    lookback_delta: Millisecond,
    interval: Millisecond,

    input: Arc<dyn ExecutionPlan>,
}

impl ExecutionPlan for SeriesNormalizeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
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
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(SeriesNormalizeStream {
            start: self.start,
            end: self.end,
            offset: self.offset,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            schema: input.schema(),
            input,
        }))
    }

    fn statistics(&self) -> datafusion::common::Statistics {
        todo!()
    }
}

pub struct SeriesNormalizeStream {
    start: Millisecond,
    end: Millisecond,
    offset: Millisecond,
    lookback_delta: Millisecond,
    interval: Millisecond,

    schema: SchemaRef,
    input: SendableRecordBatchStream,
}

impl SeriesNormalizeStream {
    /// Normalize the input record batch. Notice that for simplicity, this method assumes
    /// the input batch only contains sample points from one time series, and the value
    /// column's name is [`VALUE_COLUMN_NAME`] and timestamp column's name is [`TIMESTAMP_COLUMN_NAME`].
    ///
    /// Roughly speaking, this method does these things:
    /// - pick the sample point for each timestamp
    /// - lookback if the required timestamp doesn't have sample
    /// - bias sample's timestamp by offset
    pub fn normalize(&self, input: RecordBatch) -> ArrowResult<RecordBatch> {
        let mut take_indices = Vec::with_capacity(input.num_rows());
        let ts_column_idx = self
            .schema
            .column_with_name(TIMESTAMP_COLUMN_NAME)
            .expect("timestamp column not found")
            .0;
        // todo: maybe the input is not timestamp millisecond array
        let ts_column = input
            .column(ts_column_idx)
            .as_any()
            .downcast_ref::<Arc<PrimitiveArray<TimestampMillisecondType>>>()
            .unwrap();

        // prepare two vectors
        let mut cursor = 0;
        let expected_iter = (self.start..self.end).step_by(self.interval as usize);

        // calculate the offsets to take
        for expected_ts in expected_iter {
            'next:
            // first, search toward end to see if there is matched timestamp
            while cursor < ts_column.len() {
                let curr = ts_column.value(cursor);
                if curr == expected_ts {
                    take_indices.push(Some(cursor));
                    break 'next;
                } else if curr > expected_ts {
                    break;
                }
                cursor += 1;
            }
            // then, search backward to lookback
            loop {
                let curr = ts_column.value(cursor);
                if curr + self.lookback_delta < expected_ts {
                    // not found in lookback, leave this field blank
                    take_indices.push(None);
                    break;
                } else if curr < expected_ts && curr + self.lookback_delta >= expected_ts {
                    // find the expected value, push and break
                    take_indices.push(Some(cursor));
                    break;
                } else if cursor == 0 {
                    // reach the first value and not found in lookback, leave this field blank
                    break;
                }
                cursor -= 1;
            }
        }

        // take record batch
        let record_batch = utils::take_record_batch_optional(input, take_indices);

        // bias the timestamp column by offset
        let ts_column = record_batch
            .column(ts_column_idx)
            .as_any()
            .downcast_ref::<Arc<PrimitiveArray<TimestampMillisecondType>>>()
            .unwrap();
        let ts_column_biased = Arc::new(TimestampMillisecondArray::from_iter(
            ts_column.iter().map(|ts| ts.map(|ts| ts - self.offset)),
        ));
        let mut columns = record_batch.columns().to_vec();
        columns[ts_column_idx] = ts_column_biased;
        let new_record_batch = RecordBatch::try_new(record_batch.schema(), columns)?;

        Ok(new_record_batch)
    }
}

impl RecordBatchStream for SeriesNormalizeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SeriesNormalizeStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(batch) => {
                Poll::Ready(batch.map(|batch| batch.map(|batch| self.normalize(batch)).flatten()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
