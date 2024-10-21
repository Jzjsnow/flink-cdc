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

package com.ververica.cdc.connectors.oracle.source;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.types.DataType;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.common.source.EventSourceProvider;
import com.ververica.cdc.common.source.FlinkSourceFunctionProvider;
import com.ververica.cdc.common.source.FlinkSourceProvider;
import com.ververica.cdc.common.source.MetadataAccessor;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfig;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import com.ververica.cdc.connectors.oracle.source.meta.offset.RedoLogOffsetFactory;
import com.ververica.cdc.connectors.oracle.source.reader.OraclePipelineRecordEmitter;
import com.ververica.cdc.connectors.oracle.source.reader.OracleSourceReader;
import com.ververica.cdc.connectors.oracle.source.reader.OracleTableSourceReader;
import com.ververica.cdc.connectors.oracle.table.OracleReadableMetaData;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.table.DebeziumChangelogMode;
import com.ververica.cdc.debezium.table.MetadataConverter;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ververica.cdc.connectors.oracle.source.OracleDataSourceOptions.SCAN_STARTUP_MODE;

/**
 * A {@link DynamicTableSource} that describes how to create a Oracle redo log from a logical
 * description.
 */
public class OracleDataSource implements DataSource, SupportsReadingMetadata {

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private final OracleSourceConfig sourceConfig;
    private final Configuration config;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public OracleDataSource(OracleSourceConfigFactory configFactory, Configuration config) {
        this.sourceConfig = configFactory.create(0);
        this.config = config;
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        String url = config.get(OracleDataSourceOptions.JDBC_URL);
        int port = config.get(OracleDataSourceOptions.PORT);
        String hostname = config.get(OracleDataSourceOptions.HOSTNAME);
        String database = config.get(OracleDataSourceOptions.DATABASE);
        String username = config.get(OracleDataSourceOptions.USERNAME);
        String password = config.get(OracleDataSourceOptions.PASSWORD);
        String tableName = config.get(OracleDataSourceOptions.TABLES);
        String schemaName = config.get(OracleDataSourceOptions.SCHEMALIST);
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty(
                "database.connection.adapter",
                config.get(OracleDataSourceOptions.DATABASE_CONNECTION_ADAPTER));
        dbzProperties.setProperty(
                "log.mining.strategy", config.get(OracleDataSourceOptions.LOG_MINING_STRATEGY));
        dbzProperties.setProperty(
                "snapshot.locking.mode", config.get(OracleDataSourceOptions.SNAPSHOT_LOCKING_MODE));
        dbzProperties.setProperty(
                "database.history.store.only.captured.tables.ddl",
                config.get(OracleDataSourceOptions.HISTORY_CAPTURED_TABLES_DDL_ENABLE));
        Map<String, String> map =
                OracleDataSourceOptions.getPropertiesByPrefix(config, "debezium.");
        map.keySet().stream().forEach(e -> dbzProperties.put(e, map.get(e)));
        StartupOptions startupOptions = getStartupOptions(config);
        boolean enableParallelRead =
                config.get(OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        int splitSize = config.get(OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(OracleDataSourceOptions.CHUNK_META_GROUP_SIZE);
        int fetchSize = config.get(OracleDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        Duration connectTimeout = config.get(OracleDataSourceOptions.CONNECT_TIMEOUT);
        int connectionPoolSize = config.get(OracleDataSourceOptions.CONNECTION_POOL_SIZE);
        int connectMaxRetries = config.get(OracleDataSourceOptions.CONNECT_MAX_RETRIES);
        double distributionFactorUpper =
                config.get(OracleDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower =
                config.get(OracleDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        boolean closeIdleReaders =
                config.get(OracleDataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackfill =
                config.get(OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);

        OracleEventDeserializer deserializer =
                new OracleEventDeserializer(
                        DebeziumChangelogMode.ALL,
                        config.get(OracleDataSourceOptions.SCHEMA_CHANGE_ENABLED),
                        sourceConfig.getServerTimeZone());

        RedoLogOffsetFactory offsetFactory = new RedoLogOffsetFactory();
        if (enableParallelRead) {
            JdbcIncrementalSource<Event> oracleChangeEventSource =
                    OracleTableSourceReader.<Event>builder()
                            .hostname(hostname)
                            .url(url)
                            .port(port)
                            .databaseList(database)
                            .schemaList(schemaName)
                            .tableList(tableName)
                            .username(username)
                            .password(password)
                            .startupOptions(startupOptions)
                            .deserializer(deserializer)
                            .debeziumProperties(dbzProperties)
                            .splitSize(splitSize)
                            .splitMetaGroupSize(splitMetaGroupSize)
                            .fetchSize(fetchSize)
                            .connectTimeout(connectTimeout)
                            .connectionPoolSize(connectionPoolSize)
                            .connectMaxRetries(connectMaxRetries)
                            .distributionFactorUpper(distributionFactorUpper)
                            .distributionFactorLower(distributionFactorLower)
                            .closeIdleReaders(closeIdleReaders)
                            .skipSnapshotBackfill(skipSnapshotBackfill)
                            .recordEmitter(
                                    new OraclePipelineRecordEmitter(
                                            deserializer, true, offsetFactory, sourceConfig))
                            .build();

            return FlinkSourceProvider.of(oracleChangeEventSource);
        } else {

            OracleSourceReader.Builder<Event> builder = OracleSourceReader.<Event>builder();
            if (config.getOptional(OracleDataSourceOptions.JDBC_URL).isPresent()) {
                builder = builder.url(config.getOptional(OracleDataSourceOptions.JDBC_URL).get());
            }
            DebeziumSourceFunction<Event> sourceFunction =
                    builder.hostname(config.getOptional(OracleDataSourceOptions.HOSTNAME).get())
                            .port(config.getOptional(OracleDataSourceOptions.PORT).get())
                            .database(
                                    config.getOptional(OracleDataSourceOptions.DATABASE)
                                            .get()) // monitor  database
                            .schemaList(
                                    config.getOptional(OracleDataSourceOptions.SCHEMALIST)
                                            .get()) // monitor  schema
                            .tableList(
                                    config.getOptional(OracleDataSourceOptions.TABLES)
                                            .get()) // monitor
                            // EMP table
                            .username(config.getOptional(OracleDataSourceOptions.USERNAME).get())
                            .password(config.getOptional(OracleDataSourceOptions.PASSWORD).get())
                            .deserializer(deserializer) // converts SourceRecord to JSON String
                            .startupOptions(StartupOptions.initial())
                            .debeziumProperties(dbzProperties)
                            .sourceConfig(sourceConfig)
                            .build();

            return FlinkSourceFunctionProvider.of(sourceFunction);
        }
    }

    private static StartupOptions getStartupOptions(Configuration config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                modeString));
        }
    }

    private MetadataConverter[] getMetadataConverters() {
        if (metadataKeys.isEmpty()) {
            return new MetadataConverter[0];
        }

        return metadataKeys.stream()
                .map(
                        key ->
                                Stream.of(OracleReadableMetaData.values())
                                        .filter(m -> m.getKey().equals(key))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(OracleReadableMetaData::getConverter)
                .toArray(MetadataConverter[]::new);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new OracleMetadataAccessor(sourceConfig);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(OracleReadableMetaData.values())
                .collect(
                        Collectors.toMap(
                                OracleReadableMetaData::getKey,
                                OracleReadableMetaData::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }
}
