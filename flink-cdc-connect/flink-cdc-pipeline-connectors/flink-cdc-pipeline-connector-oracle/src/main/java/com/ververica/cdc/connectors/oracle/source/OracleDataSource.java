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

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.annotation.VisibleForTesting;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.common.source.EventSourceProvider;
import com.ververica.cdc.common.source.FlinkSourceFunctionProvider;
import com.ververica.cdc.common.source.MetadataAccessor;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfig;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import com.ververica.cdc.connectors.oracle.source.reader.OracleSourceReader;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.table.DebeziumChangelogMode;

import java.util.Properties;

/** A {@link DataSource} for oralce cdc connector. */
@Internal
public class OracleDataSource implements DataSource {

    private final OracleSourceConfig sourceConfig;
    private final Configuration config;

    public OracleDataSource(OracleSourceConfigFactory configFactory, Configuration config) {
        this.sourceConfig = configFactory.create(0);
        this.config = config;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        OracleEventDeserializer deserializer =
                new OracleEventDeserializer(
                        DebeziumChangelogMode.ALL,
                        config.get(OracleDataSourceOptions.SCHEMA_CHANGE_ENABLED));

        Properties properties = new Properties();
        properties.put(
                "database.tablename.case.insensitive",
                config.get(OracleDataSourceOptions.DATABASE_TABLE_CASE_INSENSITIVE)); // 11g数据库适配
        properties.setProperty(
                "database.connection.adapter",
                config.get(
                        OracleDataSourceOptions.DATABASE_CONNECTION_ADAPTER)); // 要同步快，这个配置必须加，不然非常慢
        properties.setProperty(
                "log.mining.strategy", config.get(OracleDataSourceOptions.LOG_MINING_STRATEGY));
        properties.setProperty(
                "log.mining.continuous.mine",
                config.get(OracleDataSourceOptions.LOG_MINING_CONTINUOUS_MINE));
        DebeziumSourceFunction<Event> sourceFunction =
                OracleSourceReader.<Event>builder()
                        .hostname(config.getOptional(OracleDataSourceOptions.HOSTNAME).get())
                        .port(config.getOptional(OracleDataSourceOptions.PORT).get())
                        .database(
                                config.getOptional(OracleDataSourceOptions.DATABASE)
                                        .get()) // monitor  database
                        .schemaList(
                                config.getOptional(OracleDataSourceOptions.SCHEMALIST)
                                        .get()) // monitor  schema
                        .tableList(
                                config.getOptional(OracleDataSourceOptions.TABLES).get()) // monitor
                        // EMP table
                        .username(config.getOptional(OracleDataSourceOptions.USERNAME).get())
                        .password(config.getOptional(OracleDataSourceOptions.PASSWORD).get())
                        .deserializer(deserializer) // converts SourceRecord to JSON String
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(properties)
                        .sourceConfig(sourceConfig)
                        .build();

        return FlinkSourceFunctionProvider.of(sourceFunction);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new OracleMetadataAccessor(sourceConfig);
    }

    @VisibleForTesting
    public OracleSourceConfig getSourceConfig() {
        return sourceConfig;
    }
}
