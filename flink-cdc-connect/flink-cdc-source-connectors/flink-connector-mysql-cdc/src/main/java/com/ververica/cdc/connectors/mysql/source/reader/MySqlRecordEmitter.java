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

package com.ververica.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import com.ververica.cdc.connectors.mysql.source.split.SourceRecords;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import io.debezium.document.Array;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getBinlogPosition;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getFetchTimestamp;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getHistoryRecord;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getMessageTimestamp;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getRecordBytes;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getWatermark;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isDataChangeRecord;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isHeartbeatEvent;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isHighWatermarkEvent;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isSchemaChangeEvent;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isWatermarkEvent;
import static org.apache.commons.math3.util.Precision.round;

/**
 * The {@link RecordEmitter} implementation for {@link MySqlSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the binlog reader to
 * emit records rather than emit the records directly.
 */
public class MySqlRecordEmitter<T> implements RecordEmitter<SourceRecords, T, MySqlSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlRecordEmitter.class);
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    private final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;
    private final MySqlSourceReaderMetrics sourceReaderMetrics;
    private final boolean includeSchemaChanges;
    private final OutputCollector<T> outputCollector;
    private double counter = 0;
    private double size = 0.0;
    private Long firstEventTimestamp = null;

    public MySqlRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            MySqlSourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges) {
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
        this.sourceReaderMetrics = sourceReaderMetrics;
        this.includeSchemaChanges = includeSchemaChanges;
        this.outputCollector = new OutputCollector<>();
    }

    @Override
    public void emitRecord(
            SourceRecords sourceRecords, SourceOutput<T> output, MySqlSplitState splitState)
            throws Exception {
        final Iterator<SourceRecord> elementIterator = sourceRecords.iterator();
        while (elementIterator.hasNext()) {
            processElement(elementIterator.next(), output, splitState);
        }
    }

    protected void processElement(
            SourceRecord element, SourceOutput<T> output, MySqlSplitState splitState)
            throws Exception {
        // timestamp for the first record arrived
        if (firstEventTimestamp == null) {
            firstEventTimestamp = System.currentTimeMillis();
        }
        if (isWatermarkEvent(element)) {
            BinlogOffset watermark = getWatermark(element);
            if (isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            }
        } else if (isSchemaChangeEvent(element) && splitState.isBinlogSplitState()) {
            HistoryRecord historyRecord = getHistoryRecord(element);
            Array tableChanges =
                    historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
            TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
            for (TableChanges.TableChange tableChange : changes) {
                splitState.asBinlogSplitState().recordSchema(tableChange.getId(), tableChange);
            }
            if (includeSchemaChanges) {
                BinlogOffset position = getBinlogPosition(element);
                splitState.asBinlogSplitState().setStartingOffset(position);
                emitElement(element, output);
            }
        } else if (isDataChangeRecord(element)) {
            updateStartingOffsetForSplit(splitState, element);
            counter++;
            size += getRecordBytes(element);
            reportMetrics(element);
            emitElement(element, output);
        } else if (isHeartbeatEvent(element)) {
            updateStartingOffsetForSplit(splitState, element);
        } else {
            // unknown element
            LOG.info("Meet unknown element {}, just skip.", element);
        }
    }

    private void updateStartingOffsetForSplit(MySqlSplitState splitState, SourceRecord element) {
        if (splitState.isBinlogSplitState()) {
            BinlogOffset position = getBinlogPosition(element);
            splitState.asBinlogSplitState().setStartingOffset(position);
        }
    }

    private void emitElement(SourceRecord element, SourceOutput<T> output) throws Exception {
        outputCollector.output = output;
        debeziumDeserializationSchema.deserialize(element, outputCollector);
    }

    private void reportMetrics(SourceRecord element) {

        Long messageTimestamp = getMessageTimestamp(element);

        if (messageTimestamp != null && messageTimestamp > 0L) {
            // report mysql fetch delay
            Long fetchTimestamp = getFetchTimestamp(element);
            if (fetchTimestamp != null && fetchTimestamp >= messageTimestamp) {
                // report mysql fetch delay
                sourceReaderMetrics.recordFetchDelay(fetchTimestamp - messageTimestamp);
            }
        }
        // report mysql num of records in
        sourceReaderMetrics.recordNumRecordsIn(counter);
        // report mysql num of bytes in
        sourceReaderMetrics.recordNumBytesIn(size);
        long currentTimestamp = System.currentTimeMillis();
        if (firstEventTimestamp == null) {
            sourceReaderMetrics.recordNumRecordsInRate(0.0);
            sourceReaderMetrics.recordNumBytesInRate(0.0);
        } else {
            double rateOfRecords = counter / (currentTimestamp - firstEventTimestamp) * 1000;
            double rateOfBytes = size / (currentTimestamp - firstEventTimestamp) * 1000;
            rateOfRecords = round(rateOfRecords, 2);
            rateOfBytes = round(rateOfBytes, 2);
            // report mysql num of records in rate
            sourceReaderMetrics.recordNumRecordsInRate(rateOfRecords);
            // report mysql num of bytes in rate
            sourceReaderMetrics.recordNumBytesInRate(rateOfBytes);
        }
    }

    private static class OutputCollector<T> implements Collector<T> {
        private SourceOutput<T> output;

        @Override
        public void collect(T record) {
            output.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
