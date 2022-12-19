//! This should be contained in promql-parser crate

use std::time::{Duration, Instant};

use promql_parser::parser::Expr as PromExpr;

pub struct EvalStmt {
    expr: PromExpr,
    start: Instant,
    end: Instant,
    interval: Duration,
    lookback_delta: Duration,
}
