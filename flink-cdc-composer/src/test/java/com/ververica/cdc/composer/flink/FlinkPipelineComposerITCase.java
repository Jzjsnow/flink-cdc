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

package com.ververica.cdc.composer.flink;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import com.ververica.cdc.common.pipeline.SchemaChangeBehavior;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.composer.PipelineExecution;
import com.ververica.cdc.composer.definition.PipelineDef;
import com.ververica.cdc.composer.definition.RouteDef;
import com.ververica.cdc.composer.definition.SinkDef;
import com.ververica.cdc.composer.definition.SourceDef;
import com.ververica.cdc.connectors.values.ValuesDatabase;
import com.ververica.cdc.connectors.values.factory.ValuesDataFactory;
import com.ververica.cdc.connectors.values.sink.ValuesDataSinkOptions;
import com.ververica.cdc.connectors.values.source.ValuesDataSourceHelper;
import com.ververica.cdc.connectors.values.source.ValuesDataSourceOptions;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.ververica.cdc.connectors.values.source.ValuesDataSourceHelper.TABLE_1;
import static com.ververica.cdc.connectors.values.source.ValuesDataSourceHelper.TABLE_2;
import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link FlinkPipelineComposer}. */
class FlinkPipelineComposerITCase {
    private static final int MAX_PARALLELISM = 4;

    // Always use parent-first classloader for CDC classes.
    // The reason is that ValuesDatabase uses static field for holding data, we need to make sure
    // the class is loaded by AppClassloader so that we can verify data in the test case.
    private static final org.apache.flink.configuration.Configuration MINI_CLUSTER_CONFIG =
            new org.apache.flink.configuration.Configuration();

    static {
        MINI_CLUSTER_CONFIG.set(
                ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("com.ververica.cdc"));
    }

    /**
     * Use {@link MiniClusterExtension} to reduce the overhead of restarting the MiniCluster for
     * every test case.
     */
    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(MAX_PARALLELISM)
                            .setConfiguration(MINI_CLUSTER_CONFIG)
                            .build());

    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();

    @BeforeEach
    void init() {
        // Take over STDOUT as we need to check the output of values sink
        System.setOut(new PrintStream(outCaptor));
        // Initialize in-memory database
        ValuesDatabase.clear();
    }

    @AfterEach
    void cleanup() {
        System.setOut(standardOut);
    }

    @Test
    void testSingleSplitSingleTable() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_SINGLE_TABLE);
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        List<SourceDef> sourceDefs = new ArrayList<>();
        sourceDefs.add(sourceDef);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDefs,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check result in ValuesDatabase
        List<String> results = ValuesDatabase.getResults(TABLE_1);
        assertThat(results)
                .contains(
                        "default_namespace.default_schema.table1:col1=2;newCol3=x",
                        "default_namespace.default_schema.table1:col1=3;newCol3=");

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        for (int i = 0; i < outputEvents.length; i++) {
            outputEvents[i] = outputEvents[i].trim();
        }
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existingColumn=null}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumns=[`newCol2` STRING]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, ], after=[2, x], op=UPDATE, meta=()}");
    }

    @Test
    void testSingleSplitMultipleTables() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_MULTI_TABLES);
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        List<SourceDef> sourceDefs = new ArrayList<>();
        sourceDefs.add(sourceDef);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDefs,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check result in ValuesDatabase
        List<String> table1Results = ValuesDatabase.getResults(TABLE_1);
        assertThat(table1Results)
                .containsExactly(
                        "default_namespace.default_schema.table1:col1=2;newCol3=x",
                        "default_namespace.default_schema.table1:col1=3;newCol3=");
        List<String> table2Results = ValuesDatabase.getResults(TABLE_2);
        assertThat(table2Results)
                .contains(
                        "default_namespace.default_schema.table2:col1=1;col2=1",
                        "default_namespace.default_schema.table2:col1=2;col2=2",
                        "default_namespace.default_schema.table2:col1=3;col2=3");

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        for (int i = 0; i < outputEvents.length; i++) {
            outputEvents[i] = outputEvents[i].trim();
        }
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.table2, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existingColumn=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumns=[`newCol2` STRING]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, 2], after=[2, x], op=UPDATE, meta=()}");
    }

    @Test
    void testMultiSplitsSingleTable() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.MULTI_SPLITS_SINGLE_TABLE);
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, MAX_PARALLELISM);
        List<SourceDef> sourceDefs = new ArrayList<>();
        sourceDefs.add(sourceDef);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDefs,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check result in ValuesDatabase
        List<String> table1Results = ValuesDatabase.getResults(TABLE_1);
        assertThat(table1Results)
                .contains(
                        "default_namespace.default_schema.table1:col1=1;col2=1;col3=x",
                        "default_namespace.default_schema.table1:col1=3;col2=3;col3=x",
                        "default_namespace.default_schema.table1:col1=5;col2=5;col3=");
    }

    @Test
    void testOneToOneRouting() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_MULTI_TABLES);
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup route
        TableId routedTable1 = TableId.tableId("default_namespace", "default_schema", "routed1");
        TableId routedTable2 = TableId.tableId("default_namespace", "default_schema", "routed2");
        List<RouteDef> routeDef =
                Arrays.asList(
                        new RouteDef(TABLE_1.toString(), routedTable1.toString(), null),
                        new RouteDef(TABLE_2.toString(), routedTable2.toString(), null));

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef, sinkDef, routeDef, Collections.emptyList(), pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check result in ValuesDatabase
        List<String> routed1Results = ValuesDatabase.getResults(routedTable1);
        assertThat(routed1Results)
                .contains(
                        "default_namespace.default_schema.routed1:col1=2;newCol3=x",
                        "default_namespace.default_schema.routed1:col1=3;newCol3=");
        List<String> routed2Results = ValuesDatabase.getResults(routedTable2);
        assertThat(routed2Results)
                .contains(
                        "default_namespace.default_schema.routed2:col1=1;col2=1",
                        "default_namespace.default_schema.routed2:col1=2;col2=2",
                        "default_namespace.default_schema.routed2:col1=3;col2=3");

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        for (int i = 0; i < outputEvents.length; i++) {
            outputEvents[i] = outputEvents[i].trim();
        }
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.routed1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.routed2, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed1, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed1, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed1, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.routed1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existingColumn=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed2, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed2, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed2, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.routed1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.routed1, droppedColumns=[`newCol2` STRING]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed1, before=[1, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed1, before=[2, 2], after=[2, x], op=UPDATE, meta=()}");
    }

    @Test
    void testMergingWithRoute() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId myTable1 = TableId.tableId("default_namespace", "default_schema", "mytable1");
        TableId myTable2 = TableId.tableId("default_namespace", "default_schema", "mytable2");
        Schema table1Schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("id")
                        .build();
        Schema table2Schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.VARCHAR(255))
                        .physicalColumn("age", DataTypes.TINYINT())
                        .physicalColumn("description", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        // Create test dataset:
        // Create table 1 [id, name, age]
        // Table 1: +I[1, Alice, 18]
        // Table 1: +I[2, Bob, 20]
        // Table 1: -U[2, Bob, 20] +U[2, Bob, 30]
        // Create table 2 [id, name, age]
        // Table 2: +I[3, Charlie, 15, student]
        // Table 2: +I[4, Donald, 25, student]
        // Table 2: -D[4, Donald, 25, student]
        // Rename column for table 1: name -> last_name
        // Add column for table 2: gender
        // Table 1: +I[5, Eliza, 24]
        // Table 2: +I[6, Frank, 30, student, male]
        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));
        BinaryRecordDataGenerator table2dataGenerator =
                new BinaryRecordDataGenerator(
                        table2Schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(myTable1, table1Schema));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice"), 18})));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob"), 20})));
        events.add(
                DataChangeEvent.updateEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob"), 20}),
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob"), 30})));
        events.add(new CreateTableEvent(myTable2, table2Schema));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    3L,
                                    BinaryStringData.fromString("Charlie"),
                                    (byte) 15,
                                    BinaryStringData.fromString("student")
                                })));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    4L,
                                    BinaryStringData.fromString("Donald"),
                                    (byte) 25,
                                    BinaryStringData.fromString("student")
                                })));
        events.add(
                DataChangeEvent.deleteEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    4L,
                                    BinaryStringData.fromString("Donald"),
                                    (byte) 25,
                                    BinaryStringData.fromString("student")
                                })));
        events.add(new RenameColumnEvent(myTable1, ImmutableMap.of("name", "last_name")));
        events.add(
                new AddColumnEvent(
                        myTable2,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("gender", DataTypes.STRING())))));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {5, BinaryStringData.fromString("Eliza"), 24})));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable2,
                        new BinaryRecordDataGenerator(
                                        new DataType[] {
                                            DataTypes.BIGINT(),
                                            DataTypes.VARCHAR(255),
                                            DataTypes.TINYINT(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING()
                                        })
                                .generate(
                                        new Object[] {
                                            6L,
                                            BinaryStringData.fromString("Frank"),
                                            (byte) 30,
                                            BinaryStringData.fromString("student"),
                                            BinaryStringData.fromString("male")
                                        })));

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup route
        TableId mergedTable = TableId.tableId("default_namespace", "default_schema", "merged");
        List<RouteDef> routeDef =
                Collections.singletonList(
                        new RouteDef(
                                "default_namespace.default_schema.mytable[0-9]",
                                mergedTable.toString(),
                                null));

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef, sinkDef, routeDef, Collections.emptyList(), pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();
        Schema mergedTableSchema = ValuesDatabase.getTableSchema(mergedTable);
        assertThat(mergedTableSchema)
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.BIGINT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.BIGINT())
                                .physicalColumn("description", DataTypes.STRING())
                                .physicalColumn("last_name", DataTypes.STRING())
                                .physicalColumn("gender", DataTypes.STRING())
                                .primaryKey("id")
                                .build());
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        for (int i = 0; i < outputEvents.length; i++) {
            outputEvents[i] = outputEvents[i].trim();
        }
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.merged, schema=columns={`id` INT,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[1, Alice, 18], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[2, Bob, 20], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[2, Bob, 20], after=[2, Bob, 30], op=UPDATE, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`description` STRING, position=LAST, existingColumn=null}]}",
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.merged, nameMapping={age=BIGINT, id=BIGINT}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[3, Charlie, 15, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[4, Donald, 25, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[4, Donald, 25, student], after=[], op=DELETE, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`last_name` STRING, position=LAST, existingColumn=null}]}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`gender` STRING, position=LAST, existingColumn=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[5, null, 24, null, Eliza, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[6, Frank, 30, student, null, male], op=INSERT, meta=()}");
    }
}
