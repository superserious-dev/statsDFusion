use async_trait::async_trait;
use datafusion::{
    arrow::{datatypes::SchemaRef, ipc::reader::StreamReader, record_batch::RecordBatch},
    catalog::Session,
    common::{Result, error::DataFusionError, project_schema},
    datasource::{TableProvider, TableType},
    execution::RecordBatchStream,
    logical_expr::expr::Expr,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
    },
};
use futures::Stream;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::{
    any::Any,
    fmt::Formatter,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

#[derive(Clone)]
pub struct RedbTable {
    db: Arc<Mutex<Database>>,
    name: String,
    table_definition: TableDefinition<'static, i64, Vec<u8>>,
    record_batch_schema: SchemaRef,
}

impl RedbTable {
    pub fn new(
        db: Arc<Mutex<Database>>,
        name: &'static str,
        record_batch_schema: SchemaRef,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            db,
            name: name.to_string(),
            table_definition: TableDefinition::new(name),
            record_batch_schema,
        })
    }
}

impl std::fmt::Debug for RedbTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("RedbTable: {}", self.name))
    }
}

#[async_trait]
impl TableProvider for RedbTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.record_batch_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = RedbTableScanExec::new(Arc::new(self.clone()), projection);
        Ok(Arc::new(scan))
    }
}

pub struct RedbRecordBatchStream {
    redb_table: Arc<RedbTable>,
    projection: Option<Vec<usize>>,
    last_seen_key: Option<i64>,
    finished: bool,
}

impl RedbRecordBatchStream {
    fn new(projection: &Option<Vec<usize>>, redb_table: Arc<RedbTable>) -> Self {
        Self {
            projection: projection.clone(),
            redb_table,
            last_seen_key: None,
            finished: false,
        }
    }

    fn get_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while !self.finished {
            let db =
                self.redb_table.db.lock().map_err(|_| {
                    DataFusionError::External("Failed to acquire database lock".into())
                })?;

            let txn = db.begin_read().map_err(|e| {
                DataFusionError::External(format!("Failed to start read transaction: {e}").into())
            })?;

            let table = txn
                .open_table(self.redb_table.table_definition)
                .map_err(|e| {
                    DataFusionError::External(format!("Failed to open table: {e}").into())
                })?;

            let mut iter = if let Some(last_seen_key) = self.last_seen_key {
                // Resume from the last seen key
                table.range(last_seen_key + 1..).map_err(|e| {
                    DataFusionError::External(format!("Failed to get table range: {e}").into())
                })?
            } else {
                // Start from the beginning
                table.iter().map_err(|e| {
                    DataFusionError::External(format!("Failed to get table iterator: {e}").into())
                })?
            };

            // Get next item
            if let Some(item) = iter.next() {
                match item {
                    Ok((key, value)) => {
                        self.last_seen_key = Some(key.value());
                        let bytes = value.value();
                        let cursor = std::io::Cursor::new(bytes);
                        match StreamReader::try_new(cursor, None) {
                            Ok(mut reader) => {
                                if let Some(batch_result) = reader.next() {
                                    match batch_result {
                                        Ok(batch) => {
                                            // Filter batch output if a projection was given, otherwise return unfiltered batch
                                            let batch = if let Some(projection) = &self.projection {
                                                batch.project(projection)?
                                            } else {
                                                batch
                                            };
                                            return Ok(Some(batch));
                                        }
                                        Err(e) => {
                                            return Err(DataFusionError::ArrowError(
                                                Box::new(e),
                                                None,
                                            ));
                                        }
                                    }
                                }
                            }
                            Err(e) => return Err(DataFusionError::ArrowError(Box::new(e), None)),
                        }
                    }
                    Err(e) => {
                        return Err(DataFusionError::External(
                            format!("Error reading from table: {e}").into(),
                        ));
                    }
                }
            } else {
                self.finished = true;
                break;
            }
        }
        Ok(None)
    }
}

impl Stream for RedbRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_next_batch() {
            Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Ok(None) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl RecordBatchStream for RedbRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        project_schema(&self.redb_table.schema(), self.projection.as_ref()).unwrap()
    }
}

pub struct RedbTableScanExec {
    redb: Arc<RedbTable>,
    cache: PlanProperties,
    projection: Option<Vec<usize>>,
}

impl RedbTableScanExec {
    fn new(redb: Arc<RedbTable>, projection: Option<&Vec<usize>>) -> Self {
        let projected_schema = project_schema(&redb.schema(), projection)
            .expect("Could not project schema during scan");
        let cache = Self::compute_properties(projected_schema);
        Self {
            redb,
            cache,
            projection: projection.cloned(),
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl ExecutionPlan for RedbTableScanExec {
    fn name(&self) -> &str {
        "RedbTableScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        let stream = RedbRecordBatchStream::new(&self.projection, Arc::clone(&self.redb));

        Ok(Box::pin(stream))
    }
}

impl std::fmt::Debug for RedbTableScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl DisplayAs for RedbTableScanExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
