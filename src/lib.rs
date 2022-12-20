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
