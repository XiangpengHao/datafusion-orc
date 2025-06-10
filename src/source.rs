// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! OrcSource implementation for reading ORC files

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::datasource::physical_plan::{FileSource, FileScanConfig};
use datafusion::datasource::physical_plan::FileOpener;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::DisplayFormatType;

use object_store::ObjectStore;

use crate::physical_exec::OrcOpener;

/// Execution plan source for reading one or more ORC files.
///
/// This provides a minimal implementation of an ORC data source, similar to ParquetSource
/// but adapted for ORC files. It supports basic functionality like:
/// 
/// * Concurrent reads from multiple files
/// * Projection pushdown (reading only required columns)
/// * Configurable batch sizes
///
/// # Example: Create an OrcSource
/// ```no_run
/// # use std::sync::Arc;
/// # use arrow::datatypes::Schema;
/// # use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
/// # use datafusion_orc::source::OrcSource;
/// # use datafusion_datasource::PartitionedFile;
/// # use datafusion_execution::object_store::ObjectStoreUrl;
/// # use datafusion_datasource::source::DataSourceExec;
///
/// # let file_schema = Arc::new(Schema::empty());
/// # let object_store_url = ObjectStoreUrl::local_filesystem();
/// let source = Arc::new(OrcSource::default());
/// // Create a DataSourceExec for reading `file1.orc` with a file size of 100MB
/// let config = FileScanConfigBuilder::new(object_store_url, file_schema, source)
///    .with_file(PartitionedFile::new("file1.orc", 100*1024*1024)).build();
/// let exec = DataSourceExec::from_data_source(config);
/// ```
#[derive(Clone, Default, Debug)]
pub struct OrcSource {
    /// Optional metrics
    pub(crate) metrics: ExecutionPlanMetricsSet,
    /// Batch size configuration
    pub(crate) batch_size: Option<usize>,
    /// Optional projected statistics
    pub(crate) projected_statistics: Option<Statistics>,
}

impl OrcSource {
    /// Create a new OrcSource with default configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the batch size for reading ORC files
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Set projected statistics
    pub fn with_statistics(mut self, statistics: Statistics) -> Self {
        self.projected_statistics = Some(statistics);
        self
    }
}

impl FileSource for OrcSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        let projection = base_config
            .file_column_projection_indices()
            .unwrap_or_else(|| (0..base_config.file_schema.fields().len()).collect());

        Arc::new(OrcOpener {
            _partition_index: partition,
            projection,
            batch_size: self
                .batch_size
                .expect("Batch size must be set before creating OrcOpener"),
            _limit: base_config.limit,
            table_schema: Arc::clone(&base_config.file_schema),
            _metrics: self.metrics.clone(),
            object_store,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, _schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> datafusion::common::Result<Statistics> {
        let statistics = &self.projected_statistics;
        let statistics = statistics
            .clone()
            .expect("projected_statistics must be set");
        Ok(statistics)
    }

    fn file_type(&self) -> &str {
        "orc"
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "")
            }
            DisplayFormatType::TreeRender => {
                Ok(())
            }
        }
    }
}

 