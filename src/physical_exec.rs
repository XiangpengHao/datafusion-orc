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

use std::sync::Arc;

use arrow::error::ArrowError;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion::error::Result;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

use orc_rust::projection::ProjectionMask;
use orc_rust::ArrowReaderBuilder;

use futures_util::StreamExt;
use object_store::ObjectStore;

use super::object_store_reader::ObjectStoreReader;

// TODO: make use of the unused fields (e.g. implement metrics)
pub struct OrcOpener {
    pub _partition_index: usize,
    pub projection: Vec<usize>,
    pub batch_size: usize,
    pub _limit: Option<usize>,
    pub table_schema: SchemaRef,
    pub _metrics: ExecutionPlanMetricsSet,
    pub object_store: Arc<dyn ObjectStore>,
}

impl FileOpener for OrcOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let reader =
            ObjectStoreReader::new(self.object_store.clone(), file_meta.object_meta.clone());
        let batch_size = self.batch_size;
        let projected_schema = SchemaRef::from(self.table_schema.project(&self.projection)?);

        Ok(Box::pin(async move {
            let mut builder = ArrowReaderBuilder::try_new_async(reader)
                .await
                .map_err(ArrowError::from)?;
            // Find complex data type column index as projection
            let mut projection = Vec::with_capacity(projected_schema.fields().len());
            for named_column in builder.file_metadata().root_data_type().children() {
                if let Some((_table_idx, _table_field)) =
                    projected_schema.fields().find(named_column.name())
                {
                    projection.push(named_column.data_type().column_index());
                }
            }
            let projection_mask =
                ProjectionMask::roots(builder.file_metadata().root_data_type(), projection);
            if let Some(range) = file_meta.range.clone() {
                let range = range.start as usize..range.end as usize;
                builder = builder.with_file_byte_range(range);
            }
            let reader = builder
                .with_batch_size(batch_size)
                .with_projection(projection_mask)
                .build_async();

            Ok(reader.boxed())
        }))
    }
}
