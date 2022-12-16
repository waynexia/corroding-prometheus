use std::sync::Arc;

use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::sql::planner::ContextProvider;
use promql_parser::parser::Expr as PromExpr;

pub struct PromQLPlanner {
    plan_builder: LogicalPlanBuilder,
    context_provider: Arc<dyn ContextProvider>,
}

impl PromQLPlanner {
    fn prom_to_plan(&self, prom_expr: PromExpr) -> Result<LogicalPlan, ()> {
        todo!()
    }
}
