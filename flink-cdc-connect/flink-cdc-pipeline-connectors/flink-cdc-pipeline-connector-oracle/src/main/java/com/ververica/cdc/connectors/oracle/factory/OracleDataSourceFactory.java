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

package com.ververica.cdc.connectors.oracle.factory;

import org.apache.flink.table.api.ValidationException;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.DataSourceFactory;
import com.ververica.cdc.common.factories.Factory;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.source.OracleDataSource;
import com.ververica.cdc.connectors.oracle.source.OracleDataSourceOptions;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.base.utils.ObjectUtils.doubleCompare;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link Factory} to create {@link OracleDataSource}. */
@Internal
public class OracleDataSourceFactory implements DataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(OracleDataSourceFactory.class);

    public static final String IDENTIFIER = "oracle";

    @Override
    public DataSource createDataSource(Context context) {
        final Configuration config = context.getFactoryConfiguration();
        int fetchSize = config.get(OracleDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        int splitSize = config.get(OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(OracleDataSourceOptions.CHUNK_META_GROUP_SIZE);

        double distributionFactorUpper =
                config.get(OracleDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower =
                config.get(OracleDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        int connectMaxRetries = config.get(OracleDataSourceOptions.CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(OracleDataSourceOptions.CONNECTION_POOL_SIZE);
        validateIntegerOption(
                OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
        validateIntegerOption(OracleDataSourceOptions.CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
        validateIntegerOption(OracleDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
        validateIntegerOption(OracleDataSourceOptions.CONNECTION_POOL_SIZE, connectionPoolSize, 1);
        validateIntegerOption(OracleDataSourceOptions.CONNECT_MAX_RETRIES, connectMaxRetries, 0);
        validateDistributionFactorUpper(distributionFactorUpper);
        validateDistributionFactorLower(distributionFactorLower);

        OracleSourceConfigFactory configFactory =
                (OracleSourceConfigFactory)
                        new OracleSourceConfigFactory()
                                .hostname(
                                        config.getOptional(OracleDataSourceOptions.HOSTNAME).get())
                                .port(config.getOptional(OracleDataSourceOptions.PORT).get())
                                .databaseList(
                                        config.getOptional(OracleDataSourceOptions.DATABASE)
                                                .get()) // monitor oracledatabase
                                .tableList(
                                        config.getOptional(OracleDataSourceOptions.TABLES)
                                                .get()) // monitor productstable
                                .username(
                                        config.getOptional(OracleDataSourceOptions.USERNAME).get())
                                .password(
                                        config.getOptional(OracleDataSourceOptions.PASSWORD).get())
                                .includeSchemaChanges(true);
        configFactory.tableList(
                config.getOptional(OracleDataSourceOptions.TABLES).get().split(","));
        configFactory.databaseList(config.getOptional(OracleDataSourceOptions.DATABASE).get());
        configFactory.schemaList(
                new String[] {config.getOptional(OracleDataSourceOptions.DATABASE).get()});
        Duration a = Duration.ofSeconds(10);

        return new OracleDataSource(configFactory, config);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OracleDataSourceOptions.HOSTNAME);
        options.add(OracleDataSourceOptions.USERNAME);
        options.add(OracleDataSourceOptions.PASSWORD);
        options.add(OracleDataSourceOptions.TABLES);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OracleDataSourceOptions.PORT);
        options.add(OracleDataSourceOptions.SERVER_TIME_ZONE);
        options.add(OracleDataSourceOptions.SERVER_ID);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_MODE);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS);
        options.add(OracleDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(OracleDataSourceOptions.CHUNK_META_GROUP_SIZE);
        options.add(OracleDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(OracleDataSourceOptions.CONNECT_TIMEOUT);
        options.add(OracleDataSourceOptions.CONNECTION_POOL_SIZE);
        options.add(OracleDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(OracleDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(OracleDataSourceOptions.CONNECT_MAX_RETRIES);
        options.add(OracleDataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(OracleDataSourceOptions.HEARTBEAT_INTERVAL);
        options.add(OracleDataSourceOptions.SCHEMA_CHANGE_ENABLED);
        return options;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET = "specific-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(Configuration config) {
        String modeString = config.get(OracleDataSourceOptions.SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                return StartupOptions.earliest();
            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                return StartupOptions.timestamp(
                        config.get(OracleDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS));

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s, %s], but was: %s",
                                OracleDataSourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_EARLIEST,
                                SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
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
                        OracleDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND
                                .key(),
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
                        OracleDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND
                                .key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
    }

    /** Replaces the default timezone placeholder with session timezone, if applicable. */
    private static ZoneId getServerTimeZone(Configuration config) {
        final String serverTimeZone = config.get(OracleDataSourceOptions.SERVER_TIME_ZONE);
        if (serverTimeZone != null) {
            return ZoneId.of(serverTimeZone);
        } else {
            LOG.warn(
                    "{} is not set, which might cause data inconsistencies for time-related fields.",
                    OracleDataSourceOptions.SERVER_TIME_ZONE.key());
            return ZoneId.systemDefault();
        }
    }
}
