/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Map;

/**
 * Copied from Debezium project(1.9.8.final) to modify the
 * io.debezium.connector.sqlserver.SqlServerOffsetContext.Loader#load(java.util.Map) method. The
 * original method is not able to get the eventSerialNo from the offset map which stores as string.
 */
public class SqlServerOffsetContext implements OffsetContext {

    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private boolean snapshotCompleted;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    /** The index of the current event within the current transaction. */
    private long eventSerialNo;

    public SqlServerOffsetContext(
            SqlServerConnectorConfig connectorConfig,
            TxLogPosition position,
            boolean snapshot,
            boolean snapshotCompleted,
            long eventSerialNo,
            TransactionContext transactionContext,
            IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        sourceInfo = new SourceInfo(connectorConfig);

        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getInTxLsn());
        sourceInfoSchema = sourceInfo.schema();

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        } else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.eventSerialNo = eventSerialNo;
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    public SqlServerOffsetContext(
            SqlServerConnectorConfig connectorConfig,
            TxLogPosition position,
            boolean snapshot,
            boolean snapshotCompleted) {
        this(
                connectorConfig,
                position,
                snapshot,
                snapshotCompleted,
                1,
                new TransactionContext(),
                new SignalBasedIncrementalSnapshotContext<>());
    }

    @Override
    public Map<String, ?> getOffset() {
        if (sourceInfo.isSnapshot()) {
            return Collect.hashMapOf(
                    SourceInfo.SNAPSHOT_KEY,
                    true,
                    SNAPSHOT_COMPLETED_KEY,
                    snapshotCompleted,
                    SourceInfo.COMMIT_LSN_KEY,
                    sourceInfo.getCommitLsn().toString());
        } else {
            return incrementalSnapshotContext.store(
                    transactionContext.store(
                            Collect.hashMapOf(
                                    SourceInfo.COMMIT_LSN_KEY,
                                    sourceInfo.getCommitLsn().toString(),
                                    SourceInfo.CHANGE_LSN_KEY,
                                    sourceInfo.getChangeLsn() == null
                                            ? null
                                            : sourceInfo.getChangeLsn().toString(),
                                    SourceInfo.EVENT_SERIAL_NO_KEY,
                                    eventSerialNo)));
        }
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    public TxLogPosition getChangePosition() {
        return TxLogPosition.valueOf(sourceInfo.getCommitLsn(), sourceInfo.getChangeLsn());
    }

    public long getEventSerialNo() {
        return eventSerialNo;
    }

    public void setChangePosition(TxLogPosition position, int eventCount) {
        if (getChangePosition().equals(position)) {
            eventSerialNo += eventCount;
        } else {
            eventSerialNo = eventCount;
        }
        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getInTxLsn());
        sourceInfo.setEventSerialNo(eventSerialNo);
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    public static class Loader implements OffsetContext.Loader<SqlServerOffsetContext> {

        private final SqlServerConnectorConfig connectorConfig;

        public Loader(SqlServerConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public SqlServerOffsetContext load(Map<String, ?> offset) {
            final Lsn changeLsn = Lsn.valueOf((String) offset.get(SourceInfo.CHANGE_LSN_KEY));
            final Lsn commitLsn = Lsn.valueOf((String) offset.get(SourceInfo.COMMIT_LSN_KEY));
            boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
            boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));

            // only introduced in 0.10.Beta1, so it might be not present when upgrading from earlier
            // versions
            // if value is String, convert it to Long
            long eventSerialNo;

            Object eventSerialNoObj = offset.get(SourceInfo.EVENT_SERIAL_NO_KEY);
            if (eventSerialNoObj != null) {
                if (eventSerialNoObj instanceof String) {
                    eventSerialNo = Long.parseLong((String) eventSerialNoObj);
                } else {
                    eventSerialNo = (Long) eventSerialNoObj;
                }
            } else {
                eventSerialNo = 0L;
            }

            return new SqlServerOffsetContext(
                    connectorConfig,
                    TxLogPosition.valueOf(commitLsn, changeLsn),
                    snapshot,
                    snapshotCompleted,
                    eventSerialNo,
                    TransactionContext.load(offset),
                    SignalBasedIncrementalSnapshotContext.load(offset));
        }
    }

    @Override
    public String toString() {
        return "SqlServerOffsetContext ["
                + "sourceInfoSchema="
                + sourceInfoSchema
                + ", sourceInfo="
                + sourceInfo
                + ", snapshotCompleted="
                + snapshotCompleted
                + ", eventSerialNo="
                + eventSerialNo
                + "]";
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.setTableId((TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void incrementalSnapshotEvents() {
        sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }
}
