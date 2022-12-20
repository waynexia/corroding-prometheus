use std::any::Any;
use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

use arrow::array::{Array, DictionaryArray, PrimitiveArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use datafusion::common::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;

use crate::matrix::Matrix;

#[derive(Debug)]
pub struct Increase {
    child: Arc<dyn PhysicalExpr>,
}

impl Increase {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Self { child }
    }

    pub fn name() -> &'static str {
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
        println!("{:?}", input_array.data_type());

        let array_data = input_array.data().clone();
        let matrix: Matrix = Matrix::new(Arc::new(array_data.into()));

        // calculation
        let mut result_array = Vec::with_capacity(matrix.len());
        for index in 0..matrix.len() {
            let range = matrix.get(index).unwrap();
            let range = range
                .as_any()
                .downcast_ref::<PrimitiveArray<Float64Type>>()
                .unwrap()
                .values();

            if range.len() < 2 {
                result_array.push(0.0);
                continue;
            }

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
        println!("{:?}", result);
        Ok(result)
    }
}

impl Display for Increase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PromQL Increase Function")
    }
}

impl PartialEq<dyn Any> for Increase {
    fn eq(&self, other: &dyn Any) -> bool {
        self.as_any().type_id() == other.type_id()
    }
}

impl PhysicalExpr for Increase {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self, _: &arrow::datatypes::Schema) -> datafusion::common::Result<DataType> {
        Ok(Self::return_type())
    }

    fn nullable(
        &self,
        input_schema: &arrow::datatypes::Schema,
    ) -> datafusion::common::Result<bool> {
        Ok(true)
    }

    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> datafusion::common::Result<ColumnarValue> {
        let input_columns = self.child.evaluate(batch)?;
        Self::calc(&[input_columns])
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        todo!()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        todo!()
    }
}
