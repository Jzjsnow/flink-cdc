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

package com.ververica.cdc.runtime.operators.schema;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import com.ververica.cdc.common.data.LocalZonedTimestampData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.FlushEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.pipeline.SchemaChangeBehavior;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.source.SupportedMetadataColumn;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.serializer.event.EventSerializer;
import com.ververica.cdc.runtime.testutils.operators.EventOperatorTestHarness;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link SchemaOperator}. */
public class SchemaOperatorTest {
    @Test
    void testProcessElement() throws Exception {
        final int maxParallelism = 4;
        final int parallelism = 2;
        final OperatorID opID = new OperatorID();
        final TableId tableId = TableId.tableId("testProcessElement");
        final RowType rowType = DataTypes.ROW(DataTypes.BIGINT(), DataTypes.STRING());

        List<OneInputStreamOperatorTestHarness<Event, Event>> testHarnesses = new ArrayList<>();
        for (int subtaskIndex = 0; subtaskIndex < parallelism; subtaskIndex++) {
            OneInputStreamOperatorTestHarness<Event, Event> testHarness =
                    createTestHarness(maxParallelism, parallelism, subtaskIndex, opID);
            testHarnesses.add(testHarness);
            testHarness.setup(EventSerializer.INSTANCE);
            testHarness.open();

            Map<String, String> meta = new HashMap<>();
            meta.put("subtask", String.valueOf(subtaskIndex));

            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            List<Event> testData =
                    Arrays.asList(
                            DataChangeEvent.updateEvent(
                                    tableId,
                                    generator.generate(
                                            new Object[] {1L, BinaryStringData.fromString("1")}),
                                    generator.generate(
                                            new Object[] {2L, BinaryStringData.fromString("2")}),
                                    meta),
                            DataChangeEvent.updateEvent(
                                    tableId,
                                    generator.generate(
                                            new Object[] {3L, BinaryStringData.fromString("3")}),
                                    generator.generate(
                                            new Object[] {4L, BinaryStringData.fromString("4")}),
                                    meta));
            for (Event event : testData) {
                testHarness.processElement(event, 0);
            }

            Collection<StreamRecord<Event>> result = testHarness.getRecordOutput();
            assertThat(result.stream().map(StreamRecord::getValue).collect(Collectors.toList()))
                    .isEqualTo(testData);
        }

        for (int subtaskIndex = 0; subtaskIndex < parallelism; subtaskIndex++) {
            testHarnesses.get(subtaskIndex).close();
        }
    }

    private OneInputStreamOperatorTestHarness<Event, Event> createTestHarness(
            int maxParallelism, int parallelism, int subtaskIndex, OperatorID opID)
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new SchemaOperator(Duration.ofMinutes(5), new ArrayList<>()),
                maxParallelism,
                parallelism,
                subtaskIndex,
                EventSerializer.INSTANCE,
                opID);
    }

    private EventOperatorTestHarness<SchemaOperator, Event> createTestHarness(
            SchemaOperator schemaOperator, int parallelism) {
        return new EventOperatorTestHarness<>(schemaOperator, parallelism);
    }

    @Test
    void testProcessDataChangeEventWithMetadataColumn() {
        final int parallelism = 1;
        final TableId tableId = TableId.tableId("testProcessElement");
        Tuple2<String, SupportedMetadataColumn> opTsMetaColumnName =
                new Tuple2<>("binlog_ts", new OpTsMetadataColumn());

        SchemaOperator schemaOperator =
                new SchemaOperator(
                        Duration.ofMinutes(5),
                        SchemaChangeBehavior.EVOLVE,
                        new ArrayList<>(),
                        opTsMetaColumnName);

        List<Event> inputEvents = new ArrayList<>();
        inputEvents.add(getCreateTableEvent(tableId));
        inputEvents.addAll(getDataChangeEvents(tableId));

        List<Event> output = processEvents(schemaOperator, parallelism, inputEvents);
        List<Event> expectedEvents = new ArrayList<>();
        expectedEvents.add(new FlushEvent(tableId, true));
        expectedEvents.add(getExpectedCreateTableEventWithOpTsColumn(tableId, opTsMetaColumnName));
        expectedEvents.addAll(getExpectedDataChangeEvents(tableId));

        assertThat(output).isEqualTo(expectedEvents);
    }

    @Test
    void testProcessSchemaChangeEventWithMetadataColumn() {
        final int parallelism = 1;
        final TableId tableId = TableId.tableId("testProcessElement");
        Tuple2<String, SupportedMetadataColumn> opTsMetaColumnName =
                new Tuple2<>("binlog_ts", new OpTsMetadataColumn());

        SchemaOperator schemaOperator =
                new SchemaOperator(
                        Duration.ofMinutes(5),
                        SchemaChangeBehavior.EVOLVE,
                        new ArrayList<>(),
                        opTsMetaColumnName);

        List<Event> inputEvents = new ArrayList<>();
        inputEvents.add(getCreateTableEvent(tableId));
        inputEvents.addAll(getSchemaChangeEvents(tableId));

        List<Event> output = processEvents(schemaOperator, parallelism, inputEvents);
        List<Event> expectedEvents = new ArrayList<>();
        expectedEvents.add(new FlushEvent(tableId, true));
        expectedEvents.add(getExpectedCreateTableEventWithOpTsColumn(tableId, opTsMetaColumnName));
        getExpectedSchemaChangeEvents(tableId)
                .forEach(
                        schemaChangeEvent -> {
                            expectedEvents.add(new FlushEvent(tableId, false));
                            expectedEvents.add(schemaChangeEvent);
                        });

        assertThat(output).isEqualTo(expectedEvents);
    }

    @Test
    void testAddColumnCollisionWithMetadataColumn() {
        final int parallelism = 1;
        final TableId tableId = TableId.tableId("testProcessElement");
        Tuple2<String, SupportedMetadataColumn> opTsMetaColumnName =
                new Tuple2<>("binlog_ts", new OpTsMetadataColumn());

        SchemaOperator schemaOperator =
                new SchemaOperator(
                        Duration.ofMinutes(5),
                        SchemaChangeBehavior.EVOLVE,
                        new ArrayList<>(),
                        opTsMetaColumnName);

        List<Event> inputEvents = new ArrayList<>();
        inputEvents.add(getCreateTableEvent(tableId));
        inputEvents.add(getAddColumnEventWithConflictMetaDataColumn(tableId));

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> processEvents(schemaOperator, parallelism, inputEvents),
                "Added column names conflict with metadata column names: table "
                        + tableId
                        + " already has op_ts column "
                        + opTsMetaColumnName.f0);
    }

    private List<Event> processEvents(
            SchemaOperator schemaOperator, int parallelism, List<Event> inputEvents) {
        try (EventOperatorTestHarness<SchemaOperator, Event> testHarness =
                createTestHarness(schemaOperator, parallelism)) {
            testHarness.open();
            for (Event event : inputEvents) {
                schemaOperator.processElement(new StreamRecord<>(event));
            }
            return testHarness.getOutputRecords().stream()
                    .map(StreamRecord::getValue)
                    .collect(Collectors.toList());
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CreateTableEvent getCreateTableEvent(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .comment("test")
                        .physicalColumn("col1", DataTypes.BIGINT())
                        .physicalColumn("col2", DataTypes.STRING(), "comment")
                        .metadataColumn("m1", DataTypes.BOOLEAN())
                        .metadataColumn("m2", DataTypes.DATE(), "mKey")
                        .metadataColumn("m3", DataTypes.TIMESTAMP_LTZ(), "mKey", "comment")
                        .option("option", "fake")
                        .primaryKey(Collections.singletonList("col1"))
                        .build();
        return new CreateTableEvent(tableId, schema);
    }

    private CreateTableEvent getExpectedCreateTableEventWithOpTsColumn(
            TableId tableId, Tuple2<String, SupportedMetadataColumn> opTsMetaColumnName) {
        Schema schema =
                Schema.newBuilder()
                        .comment("test")
                        .physicalColumn("col1", DataTypes.BIGINT())
                        .physicalColumn("col2", DataTypes.STRING(), "comment")
                        .metadataColumn("m1", DataTypes.BOOLEAN())
                        .metadataColumn("m2", DataTypes.DATE(), "mKey")
                        .metadataColumn("m3", DataTypes.TIMESTAMP_LTZ(), "mKey", "comment")
                        .physicalColumn(opTsMetaColumnName.f0, opTsMetaColumnName.f1.getType())
                        .option("option", "fake")
                        .primaryKey(Collections.singletonList("col1"))
                        .build();
        return new CreateTableEvent(tableId, schema);
    }

    private List<DataChangeEvent> getDataChangeEvents(TableId tableId) {
        final RowType rowType =
                DataTypes.ROW(
                        DataTypes.BIGINT(),
                        DataTypes.STRING(),
                        DataTypes.BOOLEAN(),
                        DataTypes.DATE(),
                        DataTypes.TIMESTAMP_LTZ());
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);

        Map<String, String> meta = new HashMap<>();
        meta.put("subtask", String.valueOf(0));
        meta.put("op_ts", "1735689601000");
        RecordData before =
                generator.generate(
                        new Object[] {
                            1L,
                            BinaryStringData.fromString("1"),
                            true,
                            (int)
                                    ChronoUnit.DAYS.between(
                                            LocalDate.of(1970, 1, 1), LocalDate.of(2025, 1, 1)),
                            LocalZonedTimestampData.fromInstant(
                                    ZonedDateTime.of(
                                                    LocalDateTime.of(2025, 1, 1, 0, 0, 0),
                                                    ZoneId.of("Asia/Shanghai"))
                                            .toInstant())
                        });

        Map<String, String> meta2 = new HashMap<>();
        meta2.put("subtask", String.valueOf(0));
        meta2.put("op_ts", "1735690201000");
        RecordData after =
                generator.generate(
                        new Object[] {
                            2L,
                            BinaryStringData.fromString("2"),
                            true,
                            (int)
                                    ChronoUnit.DAYS.between(
                                            LocalDate.of(1970, 1, 1), LocalDate.of(2025, 1, 1)),
                            LocalZonedTimestampData.fromInstant(
                                    ZonedDateTime.of(
                                                    LocalDateTime.of(2025, 1, 1, 0, 0, 0),
                                                    ZoneId.of("Asia/Shanghai"))
                                            .toInstant())
                        });

        Map<String, String> meta3 = new HashMap<>();
        meta3.put("subtask", String.valueOf(0));
        meta3.put("op_ts", "1735690381000");
        RecordData replace =
                generator.generate(
                        new Object[] {
                            3L,
                            BinaryStringData.fromString("3"),
                            false,
                            (int)
                                    ChronoUnit.DAYS.between(
                                            LocalDate.of(1970, 1, 1), LocalDate.of(2025, 3, 1)),
                            LocalZonedTimestampData.fromInstant(
                                    ZonedDateTime.of(
                                                    LocalDateTime.of(2025, 3, 1, 0, 0, 0),
                                                    ZoneId.of("Asia/Shanghai"))
                                            .toInstant())
                        });

        Map<String, String> meta4 = new HashMap<>();
        meta4.put("subtask", String.valueOf(0));
        meta4.put("op_ts", "1735690801000");

        return Arrays.asList(
                DataChangeEvent.insertEvent(tableId, before, meta),
                DataChangeEvent.updateEvent(tableId, before, after, meta2),
                DataChangeEvent.replaceEvent(tableId, replace, meta3),
                DataChangeEvent.deleteEvent(tableId, replace, meta4));
    }

    private List<DataChangeEvent> getExpectedDataChangeEvents(TableId tableId) {
        final RowType rowType =
                DataTypes.ROW(
                        DataTypes.BIGINT(),
                        DataTypes.STRING(),
                        DataTypes.BOOLEAN(),
                        DataTypes.DATE(),
                        DataTypes.TIMESTAMP_LTZ(),
                        DataTypes.TIMESTAMP());
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);

        Map<String, String> meta = new HashMap<>();
        meta.put("subtask", String.valueOf(0));
        meta.put("op_ts", "1735689601000");
        RecordData before =
                generator.generate(
                        new Object[] {
                            1L,
                            BinaryStringData.fromString("1"),
                            true,
                            (int)
                                    ChronoUnit.DAYS.between(
                                            LocalDate.of(1970, 1, 1), LocalDate.of(2025, 1, 1)),
                            LocalZonedTimestampData.fromInstant(
                                    ZonedDateTime.of(
                                                    LocalDateTime.of(2025, 1, 1, 0, 0, 0),
                                                    ZoneId.of("Asia/Shanghai"))
                                            .toInstant()),
                            TimestampData.fromMillis(Long.parseLong(meta.get("op_ts")))
                        });

        Map<String, String> meta2 = new HashMap<>();
        meta2.put("subtask", String.valueOf(0));
        meta2.put("op_ts", "1735690201000");
        RecordData before2 =
                generator.generate(
                        new Object[] {
                            1L,
                            BinaryStringData.fromString("1"),
                            true,
                            (int)
                                    ChronoUnit.DAYS.between(
                                            LocalDate.of(1970, 1, 1), LocalDate.of(2025, 1, 1)),
                            LocalZonedTimestampData.fromInstant(
                                    ZonedDateTime.of(
                                                    LocalDateTime.of(2025, 1, 1, 0, 0, 0),
                                                    ZoneId.of("Asia/Shanghai"))
                                            .toInstant()),
                            TimestampData.fromMillis(0L)
                        });
        RecordData after =
                generator.generate(
                        new Object[] {
                            2L,
                            BinaryStringData.fromString("2"),
                            true,
                            (int)
                                    ChronoUnit.DAYS.between(
                                            LocalDate.of(1970, 1, 1), LocalDate.of(2025, 1, 1)),
                            LocalZonedTimestampData.fromInstant(
                                    ZonedDateTime.of(
                                                    LocalDateTime.of(2025, 1, 1, 0, 0, 0),
                                                    ZoneId.of("Asia/Shanghai"))
                                            .toInstant()),
                            TimestampData.fromMillis(Long.parseLong(meta2.get("op_ts")))
                        });

        Map<String, String> meta3 = new HashMap<>();
        meta3.put("subtask", String.valueOf(0));
        meta3.put("op_ts", "1735690381000");
        RecordData replace =
                generator.generate(
                        new Object[] {
                            3L,
                            BinaryStringData.fromString("3"),
                            false,
                            (int)
                                    ChronoUnit.DAYS.between(
                                            LocalDate.of(1970, 1, 1), LocalDate.of(2025, 3, 1)),
                            LocalZonedTimestampData.fromInstant(
                                    ZonedDateTime.of(
                                                    LocalDateTime.of(2025, 3, 1, 0, 0, 0),
                                                    ZoneId.of("Asia/Shanghai"))
                                            .toInstant()),
                            TimestampData.fromMillis(Long.parseLong(meta3.get("op_ts")))
                        });

        Map<String, String> meta4 = new HashMap<>();
        meta4.put("subtask", String.valueOf(0));
        meta4.put("op_ts", "1735690801000");
        RecordData replace2 =
                generator.generate(
                        new Object[] {
                            3L,
                            BinaryStringData.fromString("3"),
                            false,
                            (int)
                                    ChronoUnit.DAYS.between(
                                            LocalDate.of(1970, 1, 1), LocalDate.of(2025, 3, 1)),
                            LocalZonedTimestampData.fromInstant(
                                    ZonedDateTime.of(
                                                    LocalDateTime.of(2025, 3, 1, 0, 0, 0),
                                                    ZoneId.of("Asia/Shanghai"))
                                            .toInstant()),
                            TimestampData.fromMillis(0L)
                        });

        return Arrays.asList(
                DataChangeEvent.insertEvent(tableId, before, meta),
                DataChangeEvent.updateEvent(tableId, before2, after, meta2),
                DataChangeEvent.replaceEvent(tableId, replace, meta3),
                DataChangeEvent.deleteEvent(tableId, replace2, meta4));
    }

    private List<SchemaChangeEvent> getSchemaChangeEvents(TableId tableId) {
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("testCol1", DataTypes.INT())),
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn(
                                                "testCol2", DataTypes.DOUBLE(), "desc"),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        Column.physicalColumn("testCol1", DataTypes.INT())),
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("testCol3", DataTypes.INT()),
                                        AddColumnEvent.ColumnPosition.FIRST,
                                        null)));

        Map<String, DataType> alterTypeMap = new HashMap<>();
        alterTypeMap.put("col1", DataTypes.STRING());
        alterTypeMap.put("col2", DataTypes.INT());
        AlterColumnTypeEvent alterColumnTypeEvent = new AlterColumnTypeEvent(tableId, alterTypeMap);

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        tableId,
                        Arrays.asList(
                                Column.metadataColumn("m1", DataTypes.TIMESTAMP()),
                                Column.metadataColumn("m2", DataTypes.DOUBLE(), "mKey"),
                                Column.metadataColumn("m3", DataTypes.DOUBLE(), "mKey", "desc")));

        Map<String, String> renameMap = new HashMap<>();
        renameMap.put("col2", "newCol2");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(tableId, renameMap);

        return Arrays.asList(
                addColumnEvent, alterColumnTypeEvent, dropColumnEvent, renameColumnEvent);
    }

    private AddColumnEvent getAddColumnEventWithConflictMetaDataColumn(TableId tableId) {
        return new AddColumnEvent(
                tableId,
                Arrays.asList(
                        new AddColumnEvent.ColumnWithPosition(
                                Column.physicalColumn("binlog_ts", DataTypes.INT()))));
    }

    private List<SchemaChangeEvent> getExpectedSchemaChangeEvents(TableId tableId) {
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("testCol1", DataTypes.INT()),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        Column.metadataColumn(
                                                "m3",
                                                DataTypes.TIMESTAMP_LTZ(),
                                                "mKey",
                                                "comment")),
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn(
                                                "testCol2", DataTypes.DOUBLE(), "desc"),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        Column.physicalColumn("testCol1", DataTypes.INT())),
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("testCol3", DataTypes.INT()),
                                        AddColumnEvent.ColumnPosition.BEFORE,
                                        Column.physicalColumn("col1", DataTypes.BIGINT()))));

        Map<String, DataType> alterTypeMap = new HashMap<>();
        alterTypeMap.put("col1", DataTypes.STRING());
        alterTypeMap.put("col2", DataTypes.INT());
        AlterColumnTypeEvent alterColumnTypeEvent = new AlterColumnTypeEvent(tableId, alterTypeMap);

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        tableId,
                        Arrays.asList(
                                Column.metadataColumn("m1", DataTypes.TIMESTAMP()),
                                Column.metadataColumn("m2", DataTypes.DOUBLE(), "mKey"),
                                Column.metadataColumn("m3", DataTypes.DOUBLE(), "mKey", "desc")));

        Map<String, String> renameMap = new HashMap<>();
        renameMap.put("col2", "newCol2");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(tableId, renameMap);

        return Arrays.asList(
                addColumnEvent, alterColumnTypeEvent, dropColumnEvent, renameColumnEvent);
    }

    /** A {@link SupportedMetadataColumn} for op_ts. */
    private static class OpTsMetadataColumn implements SupportedMetadataColumn {
        @Override
        public String getName() {
            return "op_ts";
        }

        @Override
        public DataType getType() {
            return DataTypes.TIMESTAMP().notNull();
        }

        @Override
        public Class<?> getJavaClass() {
            return TimestampData.class;
        }

        @Override
        public Object read(Map<String, String> metadata) {
            if (metadata.containsKey(getName())) {
                return TimestampData.fromMillis(Long.parseLong(metadata.get(getName())));
            }
            throw new IllegalArgumentException("op_ts doesn't exist in the metadata: " + metadata);
        }
    }
}
