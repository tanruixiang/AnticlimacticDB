use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_float64_array;
use datafusion::common::DFSchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    Extension, LogicalPlan, Projection, TableType, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore, Volatility,
};
#[allow(unused_imports)]
use datafusion::optimizer::{optimize_children, OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    project_schema, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use futures::Stream;

// This is the custom udf function body.
pub fn my_squre(args: &[ArrayRef]) -> Result<ArrayRef> {
    let tmp_arryas = as_float64_array(&args[0])?;
    let new_array = tmp_arryas
        .iter()
        .map(|array_elem| array_elem.map(|value| value * value))
        .collect::<Float64Array>();

    Ok(Arc::new(new_array))
}

// This is the custom Logical Plan Node that will be used in the Logical Plan tree.
#[derive(PartialEq, Eq, Hash, Debug)]
struct DoNothingPlanNode {
    input: LogicalPlan,
}

// This is a customized Logical Plan Node that requires the implementation of the
// UserDefinedLogicalNode trait, which for the developer's convenience is generally
// only required to implement the UserDefinedLogicalNodeCore trait.
impl UserDefinedLogicalNodeCore for DoNothingPlanNode {
    fn name(&self) -> &str {
        "DoNothingNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.input.expressions()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DoNothingNode")
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Self {
            input: inputs[0].clone(),
        }
    }
}

// Customized QueryPlanner, the main logic is to add the ExtensionPlanner that we need to expand.
// QueryPlanner is used to convert a Logical Plan into a Physical Plan.
struct DoNothingQueryPlanner {}

#[async_trait]
impl QueryPlanner for DoNothingQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(DoNothingPlanner {})]);
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

// Define how to convert customized logical plan node into physical plan node
struct DoNothingPlanner {}

#[async_trait]
impl ExtensionPlanner for DoNothingPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(_) = node.as_any().downcast_ref::<DoNothingPlanNode>() {
                assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
                assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
                Some(Arc::new(DoNothingExec {
                    input: physical_inputs[0].clone(),
                }))
            } else {
                None
            },
        )
    }
}

// Executable physical plan nodes.
struct DoNothingExec {
    input: Arc<dyn ExecutionPlan>,
}

impl Debug for DoNothingExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DoNothingxec")
    }
}

impl DisplayAs for DoNothingExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DoNothingxec ")
            }
        }
    }
}

// Execution logic for physical plan nodes
#[async_trait]
impl ExecutionPlan for DoNothingExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }
    /// ExecutionPlan's output schema
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    /// Define the output partition schema, which has three modes
    /// 1. partitioning using round-robin algorithm, specify number of partitions
    /// 2. partitioning using hashing, specify number of partitions
    /// 3. unknown partitioning method, specify number of partitions
    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    /// Whether output is ordered
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    // Data distribution requirements for child nodes
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    /// children nodes
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Returns a new plan in which all child nodes are replaced by the new plan
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DoNothingExec {
            input: children[0].clone(),
        }))
    }

    /// The specific execution function that needs to be returned is a stream
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            panic!("invalid");
        }

        Ok(Box::pin(DoNothingReader {
            input: self.input.execute(partition, context)?,
        }))
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        // to improve the optimizability of this plan
        // better statistics inference could be provided
        datafusion::physical_plan::Statistics::default()
    }
}

struct DoNothingReader {
    input: SendableRecordBatchStream,
}

impl Stream for DoNothingReader {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // you can modify here to add custom code
        self.input.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for DoNothingReader {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

// You can learn how CeresDB customizes optimization rules by looking at
// https://github.com/CeresDB/ceresdb/blob/v1.2.7/query_engine/src/datafusion_impl/physical_optimizer/repartition.rs
struct DoNothingOptimizerRule {}
impl OptimizerRule for DoNothingOptimizerRule {
    // Example rewrite pass to insert a user defined LogicalPlanNode
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        if let LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
            ..
        }) = plan
        {
            if let LogicalPlan::Extension(Extension { node }) = input.as_ref() {
                if node.name() == "DoNothingNode" {
                    return Ok(Some(plan.clone()));
                }
            }
            let plus_node = LogicalPlan::Extension(Extension {
                node: Arc::new(DoNothingPlanNode {
                    input: input.as_ref().clone(),
                }),
            });

            let new_projection = LogicalPlan::Projection(
                Projection::try_new_with_schema(expr.clone(), plus_node.into(), schema.clone())
                    .unwrap(),
            );

            return Ok(Some(new_projection));
        }
        // This function can be used to optimize rules across the entire Logical plan tree
        // optimize_children(self, plan, config)
        Ok(Some(plan.clone()))
    }

    fn name(&self) -> &str {
        "DoNothingRule"
    }
}

/// A custom TableProvider
#[derive(Clone)]
pub struct CustomDataSource {
    inner: Arc<Mutex<CustomDataSourceInner>>,
}

struct CustomDataSourceInner {
    path: String,
}
impl Default for CustomDataSource {
    fn default() -> Self {
        CustomDataSource {
            inner: Arc::new(Mutex::new(CustomDataSourceInner {
                path: "./src/test.csv".into(),
            })),
        }
    }
}

impl Debug for CustomDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("custom_csv")
    }
}

impl CustomDataSource {
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExec::new(projections, schema, self.clone())))
    }
}

// You can learn how CeresDB uses TableProvider by looking at
// https://github.com/CeresDB/ceresdb/blob/v1.2.7/table_engine/src/provider.rs

// Implement TableProvider trait, the focus is on the implementation of
// the scan interface, which needs to return an ExecutionPlan
#[async_trait]
impl TableProvider for CustomDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int64, true),
            Field::new("c4", DataType::Int64, true),
            Field::new("c5", DataType::Int64, true),
            Field::new("c6", DataType::Int64, true),
            Field::new("c7", DataType::Int64, true),
            Field::new("c8", DataType::Int64, true),
            Field::new("c9", DataType::Int64, true),
            Field::new("c10", DataType::UInt64, true),
            Field::new("c11", DataType::Float64, true),
            Field::new("c12", DataType::Float64, true),
            Field::new("c13", DataType::Utf8, true),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, self.schema()).await;
    }
}

// ExecutionPlan
#[derive(Debug, Clone)]
struct CustomExec {
    db: CustomDataSource,
    projected_indexs: Option<Vec<usize>>,
    csv_schema: SchemaRef,
}

impl CustomExec {
    fn new(projections: Option<&Vec<usize>>, schema: SchemaRef, db: CustomDataSource) -> Self {
        Self {
            db,
            projected_indexs: projections.clone().cloned(),
            csv_schema: schema,
        }
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "CustomExec")
    }
}

// ExecutionPlan logic for actual physical plan execution
impl ExecutionPlan for CustomExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        project_schema(&self.csv_schema, self.projected_indexs.as_ref()).unwrap()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let db = self.db.inner.lock().unwrap();
        let file = File::open(db.path.clone()).unwrap();
        let csv_reader = ReaderBuilder::new(Arc::new(self.csv_schema.as_ref().clone()))
            .has_header(true)
            .build(file)
            .unwrap();

        let mut combined_record_batch = RecordBatch::new_empty(self.csv_schema.clone());
        for batch in csv_reader {
            combined_record_batch = match batch {
                Ok(batch) => {
                    concat_batches(&self.csv_schema, vec![&combined_record_batch, &batch]).unwrap()
                }
                Err(_) => break,
            }
        }
        if let Some(indexs) = &self.projected_indexs {
            combined_record_batch = combined_record_batch.project(&indexs).unwrap();
        }
        Ok(Box::pin(MemoryStream::try_new(
            vec![combined_record_batch],
            self.schema(),
            None,
        )?))
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        datafusion::physical_plan::Statistics::default()
    }
}

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<()> {
    let sql =
        "select  c12, my_squre(c12) as squre from aggregate_test_100 order by squre desc limit 10";

    // create local execution context
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::with_config_rt(config, runtime)
        .with_query_planner(Arc::new(DoNothingQueryPlanner {}))
        .add_optimizer_rule(Arc::new(DoNothingOptimizerRule {}));
    let ctx = SessionContext::with_state(state);

    // register table
    let table_reference = TableReference::from("aggregate_test_100");
    let table_provider = Arc::new(CustomDataSource::default());
    ctx.register_table(table_reference, table_provider).unwrap();

    // create and register udf
    // You can see how CeresDB uses UDF by looking at https://github.com/CeresDB/ceresdb/blob/v1.2.7/df_operator/src/udfs/time_bucket.rs
    let udf = create_udf(
        "my_squre",
        vec![DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        make_scalar_function(my_squre),
    );
    ctx.register_udf(udf);

    // run the sql
    let df = ctx.sql(sql).await.unwrap();
    df.show().await.unwrap();

    Ok(())
}
