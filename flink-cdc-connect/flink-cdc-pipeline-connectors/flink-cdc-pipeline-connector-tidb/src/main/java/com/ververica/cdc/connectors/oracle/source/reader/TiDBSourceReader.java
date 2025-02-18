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

package com.ververica.cdc.connectors.oracle.source.reader;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.connectors.oracle.dto.JdbcInfo;
import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.table.StartupOptions;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;

import java.util.ArrayList;
import java.util.List;

/** A builder to build a SourceFunction which can read snapshot and continue to read CDC events. */
public class TiDBSourceReader {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TiDBSourceReader.class);

    public static TiDBSourceReader.Builder<Event> builder() {
        return new TiDBSourceReader.Builder();
    }

    /** Builder class of {@link com.ververica.cdc.connectors.tidb.TiDBSource}. */
    public static class Builder<T> {
        private StartupOptions startupOptions = StartupOptions.initial();
        private TiConfiguration tiConf;
        private JdbcInfo jdbcInfo;
        private List<TableId> tables;
        private int snapshotWorkerNums;

        private TiKVSnapshotEventDeserializationSchema<Event> snapshotEventDeserializationSchema;
        private TiKVChangeEventDeserializationSchema<Event> changeEventDeserializationSchema;

        /** TableName name to be monitored. */
        public Builder<T> tables(List<TableId> tables) {
            this.tables = tables;
            return this;
        }

        /** Number of concurrent threads for snapshots. */
        public Builder<T> snapshotWorkerNums(int snapshotWorkerNums) {
            this.snapshotWorkerNums = snapshotWorkerNums;
            return this;
        }

        /** The deserializer used to convert from consumed snapshot event from TiKV. */
        public Builder<T> snapshotEventDeserializer(
                TiKVSnapshotEventDeserializationSchema<Event> snapshotEventDeserializationSchema) {
            this.snapshotEventDeserializationSchema = snapshotEventDeserializationSchema;
            return this;
        }

        /** The deserializer used to convert from consumed change event from TiKV. */
        public Builder<T> changeEventDeserializer(
                TiKVChangeEventDeserializationSchema<Event> changeEventDeserializationSchema) {
            this.changeEventDeserializationSchema = changeEventDeserializationSchema;
            return this;
        }

        /** Specifies the startup options. */
        public Builder<T> startupOptions(StartupOptions startupOptions) {
            this.startupOptions = startupOptions;
            return this;
        }

        /** TIDB config. */
        public Builder<T> tiConf(TiConfiguration tiConf) {
            this.tiConf = tiConf;
            return this;
        }

        public Builder<T> jdbcInfo(JdbcInfo jdbcInfo) {
            this.jdbcInfo = jdbcInfo;
            return this;
        }

        public RichParallelSourceFunction<Event> build() {
            List<String> tableList = new ArrayList<>();
            for (TableId tableId : tables) {
                tableList.add(tableId.getSchemaName() + "." + tableId.getTableName());
                LOG.info("including table {} for further processing", tableId);
            }
            return new TidbParallelSourceFunction(
                    snapshotEventDeserializationSchema,
                    changeEventDeserializationSchema,
                    tiConf,
                    startupOptions.startupMode,
                    //                    tables,
                    jdbcInfo,
                    tableList,
                    snapshotWorkerNums);
        }
    }
}
