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

package com.ververica.cdc.connectors.hudi.sink;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.OperationType;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.utils.Preconditions;
import com.ververica.cdc.common.utils.SchemaUtils;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A serializer for Event to HudiRecord. */
public class HudiEventSerializer implements Serializable {
    private Map<TableId, Schema> schemaMaps = new HashMap<>();
    int count = 0;

    /** Format DATE type data. */
    public static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

    /** ZoneId from pipeline config to support timestamp with local time zone. */
    public final ZoneId pipelineZoneId;

    public HudiEventSerializer(ZoneId zoneId) {
        pipelineZoneId = zoneId;
    }

    public GenericRowData serialize(Event event, Collector<RowData> out) throws IOException {
        if (event instanceof DataChangeEvent) {
            return applyDataChangeEvent((DataChangeEvent) event, out);
        } else if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();
            if (event instanceof CreateTableEvent) {
                schemaMaps.put(tableId, ((CreateTableEvent) event).getSchema());
            } else {
                if (!schemaMaps.containsKey(tableId)) {
                    throw new RuntimeException("schema of " + tableId + " is not existed.");
                }
                schemaMaps.put(
                        tableId,
                        SchemaUtils.applySchemaChangeEvent(
                                schemaMaps.get(tableId), schemaChangeEvent));
            }
        }
        return null;
    }

    private GenericRowData applyDataChangeEvent(DataChangeEvent event, Collector<RowData> out)
            throws JsonProcessingException {
        TableId tableId = event.tableId();
        count++;
        Schema schema = schemaMaps.get(tableId);
        Preconditions.checkNotNull(schema, event.tableId() + " is not existed");
        GenericRowData rowData;
        OperationType op = event.op();
        switch (op) {
            case INSERT:
                rowData = serializerRecord(event.after(), schema);
                rowData.setRowKind(RowKind.INSERT);
                out.collect(rowData);
                break;
            case UPDATE:
            case REPLACE:
                rowData = serializerRecord(event.before(), schema);
                rowData.setRowKind(RowKind.UPDATE_BEFORE);
                out.collect(rowData);
                rowData = serializerRecord(event.after(), schema);
                rowData.setRowKind(RowKind.UPDATE_AFTER);
                out.collect(rowData);
                break;
            case DELETE:
                rowData = serializerRecord(event.before(), schema);
                rowData.setRowKind(RowKind.DELETE);
                out.collect(rowData);
                break;
            default:
                throw new UnsupportedOperationException("Unsupport Operation " + op);
        }
        return rowData;
    }

    /** serializer RecordData to hudi Value. */
    public GenericRowData serializerRecord(RecordData recordData, Schema schema) {

        List<Column> columns = schema.getColumns();
        GenericRowData genericRowData = new GenericRowData(columns.size());
        Preconditions.checkState(
                columns.size() == recordData.getArity(),
                "Column size does not match the data size");
        try {
            for (int i = 0; i < recordData.getArity(); i++) {
                HudiRowConverter.SerializationConverter converter =
                        HudiRowConverter.createNullableExternalConverter(
                                columns.get(i).getType(), pipelineZoneId);
                Object field = converter.serialize(i, recordData);
                if (field instanceof String) {
                    genericRowData.setField(i, StringData.fromString(field.toString()));
                } else {
                    genericRowData.setField(i, field);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return genericRowData;
    }
}
