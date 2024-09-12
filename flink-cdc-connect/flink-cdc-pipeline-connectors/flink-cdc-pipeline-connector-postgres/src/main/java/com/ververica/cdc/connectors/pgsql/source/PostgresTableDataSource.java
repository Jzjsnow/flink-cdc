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

package com.ververica.cdc.connectors.pgsql.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.types.DataType;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.annotation.VisibleForTesting;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.common.source.EventSourceProvider;
import com.ververica.cdc.common.source.FlinkSourceFunctionProvider;
import com.ververica.cdc.common.source.FlinkSourceProvider;
import com.ververica.cdc.common.source.MetadataAccessor;
import com.ververica.cdc.connectors.base.options.StartupMode;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.pgsql.source.reader.PgSqlPipelineRecordEmitter;
import com.ververica.cdc.connectors.pgsql.source.reader.PostgreSQLSourceReader;
import com.ververica.cdc.connectors.pgsql.source.reader.PostgreSQLTableSourceReader;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import com.ververica.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import com.ververica.cdc.connectors.postgres.table.PostgreSQLReadableMetadata;
import com.ververica.cdc.debezium.table.DebeziumChangelogMode;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ververica.cdc.common.utils.Preconditions.checkState;
import static com.ververica.cdc.connectors.base.utils.ObjectUtils.doubleCompare;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.CHANGELOG_MODE;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.CHUNK_META_GROUP_SIZE;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.CONNECTION_POOL_SIZE;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.CONNECT_MAX_RETRIES;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.CONNECT_TIMEOUT;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.DATABASE_NAME;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.DECODING_PLUGIN_NAME;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.HEARTBEAT_INTERVAL;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.HOSTNAME;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.PASSWORD;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.PG_PORT;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCHEMA_NAME;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.TABLE_NAME;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.USERNAME;

/** A {@link DataSource} for PgSQL pipeline cdc connector. */
@Internal
public class PostgresTableDataSource implements DataSource, SupportsReadingMetadata {

    private final PostgresSourceConfig sourceConfig;
    private final Configuration config;
    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public PostgresTableDataSource(
            PostgresSourceConfigFactory configFactory, Configuration config) {
        this.sourceConfig = configFactory.create(0);
        this.config = config;
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        String hostname = config.get(HOSTNAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String schemaName = config.get(SCHEMA_NAME);
        String tableName = config.get(TABLE_NAME);
        int port = config.get(PG_PORT);
        String pluginName = config.get(DECODING_PLUGIN_NAME);
        Random random = new Random();
        String slotName = "t_table_slot_" + Math.abs(random.nextInt());
        DebeziumChangelogMode changelogMode = config.get(CHANGELOG_MODE);
        boolean enableParallelRead = config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        StartupOptions startupOptions = getStartupOptions(config);
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);
        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        Duration connectTimeout = config.get(CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(CONNECTION_POOL_SIZE);
        double distributionFactorUpper = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        Duration heartbeatInterval = config.get(HEARTBEAT_INTERVAL);
        boolean closeIdlerReaders = config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackfill = config.get(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);

        if (enableParallelRead) {
            validateIntegerOption(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
            validateIntegerOption(SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
            validateIntegerOption(CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
            validateIntegerOption(
                    PostgresDataSourceOptions.CONNECTION_POOL_SIZE, connectionPoolSize, 1);
            validateIntegerOption(
                    PostgresDataSourceOptions.CONNECT_MAX_RETRIES, connectMaxRetries, 0);
            validateDistributionFactorUpper(distributionFactorUpper);
            validateDistributionFactorLower(distributionFactorLower);
        } else {
            checkState(
                    !StartupMode.LATEST_OFFSET.equals(startupOptions.startupMode),
                    "The Postgres CDC connector does not support 'latest-offset' startup mode when 'scan.incremental.snapshot.enabled' is disabled, you can enable 'scan.incremental.snapshot.enabled' to use this startup mode.");
        }
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(
                        DebeziumChangelogMode.ALL, true, sourceConfig.getServerTimeZone());
        PostgresOffsetFactory offsetFactory = new PostgresOffsetFactory();
        if (enableParallelRead) {
            JdbcIncrementalSource<Event> parallelSource =
                    PostgreSQLTableSourceReader.<Event>builder()
                            .hostname(hostname)
                            .port(port)
                            .database(databaseName)
                            .schemaList(schemaName)
                            .tableList(tableName)
                            .username(username)
                            .password(password)
                            .decodingPluginName(pluginName)
                            .slotName(slotName)
                            .deserializer(deserializer)
                            .splitSize(splitSize)
                            .splitMetaGroupSize(splitMetaGroupSize)
                            .distributionFactorUpper(distributionFactorUpper)
                            .distributionFactorLower(distributionFactorLower)
                            .fetchSize(fetchSize)
                            .connectTimeout(connectTimeout)
                            .connectMaxRetries(connectMaxRetries)
                            .connectionPoolSize(connectionPoolSize)
                            .startupOptions(startupOptions)
                            .recordEmitter(
                                    new PgSqlPipelineRecordEmitter(
                                            deserializer, true, offsetFactory, sourceConfig))
                            .heartbeatInterval(heartbeatInterval)
                            .closeIdleReaders(closeIdlerReaders)
                            .skipSnapshotBackfill(skipSnapshotBackfill)
                            .build();
            return FlinkSourceProvider.of(parallelSource);
        } else {
            SourceFunction<Event> sourceFunction =
                    PostgreSQLSourceReader.<Event>builder()
                            .hostname(hostname)
                            .port(port)
                            .database(databaseName) // monitor postgresdatabase
                            .schemaList(schemaName) // monitor inventory schema
                            .tableList(tableName) // monitor productstable
                            .username(username)
                            .password(password)
                            .decodingPluginName(pluginName) // pg解码插件
                            .slotName(slotName) // 复制槽名称 不能重复
                            .deserializer(deserializer) // converts SourceRecord to JSON String
                            .sourceConfig(sourceConfig)
                            .dropSoltEnable(config.get(PostgresDataSourceOptions.SLOT_DROP_ENABLED))
                            .heartbeatMs(config.get(PostgresDataSourceOptions.DEFAULT_HEARTBEAT_MS))
                            .build();
            return FlinkSourceFunctionProvider.of(sourceFunction);
        }
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new PostgresMetadataAccessor(sourceConfig);
    }

    @VisibleForTesting
    public PostgresSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    /** Checks the value of given integer option is valid. */
    private void validateIntegerOption(
            ConfigOption<Integer> option, int optionValue, int exclusiveMin) {
        checkState(
                optionValue > exclusiveMin,
                String.format(
                        "The value of option '%s' must larger than %d, but is %d",
                        option.key(), exclusiveMin, optionValue));
    }

    /** Checks the value of given evenly distribution factor upper bound is valid. */
    private void validateDistributionFactorUpper(double distributionFactorUpper) {
        checkState(
                doubleCompare(distributionFactorUpper, 1.0d) >= 0,
                String.format(
                        "The value of option '%s' must larger than or equals %s, but is %s",
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
                        1.0d,
                        distributionFactorUpper));
    }

    /** Checks the value of given evenly distribution factor lower bound is valid. */
    private void validateDistributionFactorLower(double distributionFactorLower) {
        checkState(
                doubleCompare(distributionFactorLower, 0.0d) >= 0
                        && doubleCompare(distributionFactorLower, 1.0d) <= 0,
                String.format(
                        "The value of option '%s' must between %s and %s inclusively, but is %s",
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
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

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(PostgreSQLReadableMetadata.values())
                .collect(
                        Collectors.toMap(
                                PostgreSQLReadableMetadata::getKey,
                                PostgreSQLReadableMetadata::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }
}
