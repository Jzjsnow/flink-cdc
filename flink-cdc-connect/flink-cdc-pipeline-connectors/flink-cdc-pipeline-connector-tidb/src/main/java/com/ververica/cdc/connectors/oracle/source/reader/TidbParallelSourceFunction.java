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

import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.connectors.oracle.dto.JdbcInfo;
import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.TiKVRichParallelSourceFunction;
import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.table.StartupMode;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;

import static com.ververica.cdc.connectors.oracle.utils.SchemaUtils.getSchema;

/** A builder to build a SourceFunction which can read snapshot and continue to read CDC events. */
public class TidbParallelSourceFunction<T> extends TiKVRichParallelSourceFunction<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(TidbParallelSourceFunction.class);
    private final String database;
    private final String tableName;
    private final JdbcInfo jdbcInfo;

    public TidbParallelSourceFunction(
            TiKVSnapshotEventDeserializationSchema<Event> snapshotEventDeserializationSchema,
            TiKVChangeEventDeserializationSchema<Event> changeEventDeserializationSchema,
            TiConfiguration tiConf,
            StartupMode startupMode,
            String database,
            String tableName,
            JdbcInfo jdbcInfo) {
        super(
                snapshotEventDeserializationSchema,
                changeEventDeserializationSchema,
                tiConf,
                startupMode,
                database,
                tableName);
        this.database = database;
        this.tableName = tableName;
        this.jdbcInfo = jdbcInfo;
    }

    @Override
    public void run(final SourceContext<Event> ctx) throws Exception {
        TableId tableId = TableId.parse(database + "." + tableName);
        Schema schema = getSchema(tableId, jdbcInfo);
        ctx.collect(
                new CreateTableEvent(
                        com.ververica.cdc.common.event.TableId.tableId(
                                tableId.catalog(), tableId.table()),
                        schema));
        super.run(ctx);
    }
}
