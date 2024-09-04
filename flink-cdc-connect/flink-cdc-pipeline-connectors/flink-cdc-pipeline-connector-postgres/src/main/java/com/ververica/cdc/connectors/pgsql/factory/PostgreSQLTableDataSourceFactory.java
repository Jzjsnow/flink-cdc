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

package com.ververica.cdc.connectors.pgsql.factory;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.DataSourceFactory;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.connectors.pgsql.source.PostgresTableDataSource;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import com.ververica.cdc.connectors.postgres.table.PostgreSQLTableSource;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.CHANGELOG_MODE;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.CHUNK_META_GROUP_SIZE;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.CONNECTION_POOL_SIZE;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.CONNECT_MAX_RETRIES;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.CONNECT_TIMEOUT;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.DATABASE_NAME;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.DECODING_PLUGIN_NAME;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.DEFAULT_HEARTBEAT_MS;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.HEARTBEAT_INTERVAL;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.HOSTNAME;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.PASSWORD;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.PG_PORT;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCHEMA_CHANGE_ENABLED;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SCHEMA_NAME;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SLOT_DROP_ENABLED;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SLOT_NAME;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.TABLE_NAME;
import static com.ververica.cdc.connectors.pgsql.source.PostgresDataSourceOptions.USERNAME;

/** Factory for creating configured instance of {@link PostgreSQLTableSource}. */
public class PostgreSQLTableDataSourceFactory implements DataSourceFactory {

    private static final String IDENTIFIER = "postgres";

    @Override
    public DataSource createDataSource(Context context) {
        final Configuration config = context.getFactoryConfiguration();
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname(config.get(HOSTNAME));
        configFactory.port(config.get(PG_PORT));
        configFactory.database(config.get(DATABASE_NAME));
        configFactory.tableList(config.get(TABLE_NAME).split(","));
        configFactory.username(config.get(USERNAME));
        configFactory.password(config.get(PASSWORD));
        configFactory.includeSchemaChanges(config.get(SCHEMA_CHANGE_ENABLED));
        Random random = new Random();
        configFactory.slotName("t_table_slot_" + Math.abs(random.nextInt()));
        configFactory.schemaList(new String[] {config.get(DATABASE_NAME)});
        configFactory.decodingPluginName(config.get(DECODING_PLUGIN_NAME));
        Duration duration = Duration.ofSeconds(10);
        configFactory.heartbeatInterval(duration);
        return new PostgresTableDataSource(configFactory, config);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<com.ververica.cdc.common.configuration.ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        options.add(SCHEMA_NAME);
        options.add(TABLE_NAME);
        options.add(SLOT_NAME);
        return options;
    }

    @Override
    public Set<com.ververica.cdc.common.configuration.ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PG_PORT);
        options.add(DECODING_PLUGIN_NAME);
        options.add(CHANGELOG_MODE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECT_MAX_RETRIES);
        options.add(CONNECTION_POOL_SIZE);
        options.add(HEARTBEAT_INTERVAL);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        options.add(SLOT_DROP_ENABLED);
        options.add(DEFAULT_HEARTBEAT_MS);
        return options;
    }
}
