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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;

import java.util.ArrayList;
import java.util.List;

import static com.ververica.cdc.connectors.oracle.utils.SchemaUtils.getSchema;

/** A builder to build a SourceFunction which can read snapshot and continue to read CDC events. */
public class TidbParallelSourceFunction<T> extends TiKVRichParallelSourceFunction<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(TidbParallelSourceFunction.class);
    private final List<CreateTableEvent> createTableEventCache;

    public TidbParallelSourceFunction(
            TiKVSnapshotEventDeserializationSchema<Event> snapshotEventDeserializationSchema,
            TiKVChangeEventDeserializationSchema<Event> changeEventDeserializationSchema,
            TiConfiguration tiConf,
            StartupMode startupMode,
            JdbcInfo jdbcInfo,
            List<String> capturedTableIds,
            int snapshotWorkerNums) {
        super(
                snapshotEventDeserializationSchema,
                changeEventDeserializationSchema,
                tiConf,
                startupMode,
                capturedTableIds,
                snapshotWorkerNums);
        this.createTableEventCache = new ArrayList<>();
        try {
            for (String tableId : capturedTableIds) {
                Schema schema = getSchema(tableId, jdbcInfo);
                createTableEventCache.add(
                        new CreateTableEvent(
                                com.ververica.cdc.common.event.TableId.tableId(
                                        tableId.split("\\.")[0], tableId.split("\\.")[1]),
                                schema));
            }
        } catch (Exception e) {
            throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
        }
    }

    @Override
    public void run(final SourceContext<Event> ctx) throws Exception {
        for (CreateTableEvent createTableEvent : createTableEventCache) {
            ctx.collect(createTableEvent);
        }
        createTableEventCache.clear();
        super.run(ctx);
    }
}
