/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.pgsql.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitState;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader;
import com.ververica.cdc.connectors.postgres.source.PostgresDialect;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import com.ververica.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

/**
 * The basic source of Incremental Snapshot framework for JDBC datasource, it is based on FLIP-27
 * and Watermark Signal Algorithm which supports parallel reading snapshot of table and then
 * continue to capture data change by streaming reading.
 */
public class PostgreSQLTableSourceReader<T>
        extends PostgresSourceBuilder.PostgresIncrementalSource<T> {

    public PostgreSQLTableSourceReader(
            PostgresSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            PostgresOffsetFactory offsetFactory,
            PostgresDialect dataSourceDialect,
            RecordEmitter<SourceRecords, T, SourceSplitState> recordEmitter) {
        super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
    }

    @Override
    public IncrementalSourceReader createReader(SourceReaderContext readerContext)
            throws Exception {
        return super.createReader(readerContext);
    }
}
