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

package com.ververica.cdc.connectors.pgsql.source.reader;

import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.Validator;
import io.debezium.connector.postgresql.PostgresConnector;

import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume binlog for
 * PostgreSQL.
 */
public class PostgreSQLSourceReader {

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link PostgreSQLSourceReader}. */
    public static class Builder<T> {

        private String pluginName = "decoderbufs";
        private String slotName = "flink";
        private int port = 5432; // default 5432 port
        private String hostname;
        private String database;
        private String username;
        private String password;
        private String[] schemaList;
        private String[] tableList;
        private Properties dbzProperties;
        private DebeziumDeserializationSchema<T> deserializer;
        private PostgresSourceConfig sourceConfig;
        private String dropEnable;
        private String defaultHeartbeatMs;

        /**
         * The name of the Postgres logical decoding plug-in installed on the server. Supported
         * values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming,
         * wal2json_rds_streaming and pgoutput.
         */
        public Builder<T> decodingPluginName(String name) {
            this.pluginName = name;
            return this;
        }

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder<T> dropSoltEnable(String dropEnable) {
            this.dropEnable = dropEnable;
            return this;
        }

        public Builder<T> sourceConfig(PostgresSourceConfig sourceConfig) {
            this.sourceConfig = sourceConfig;
            return this;
        }

        /** Integer port number of the PostgreSQL database server. */
        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        /** The name of the PostgreSQL database from which to stream the changes. */
        public Builder<T> database(String database) {
            this.database = database;
            return this;
        }

        /**
         * An optional list of regular expressions that match schema names to be monitored; any
         * schema name not included in the whitelist will be excluded from monitoring. By default
         * all non-system schemas will be monitored.
         */
        public Builder<T> schemaList(String... schemaList) {
            this.schemaList = schemaList;
            return this;
        }

        /**
         * An optional list of regular expressions that match fully-qualified table identifiers for
         * tables to be monitored; any table not included in the whitelist will be excluded from
         * monitoring. Each identifier is of the form schemaName.tableName. By default the connector
         * will monitor every non-system table in each monitored schema.
         */
        public Builder<T> tableList(String... tableList) {
            this.tableList = tableList;
            return this;
        }

        /**
         * Name of the PostgreSQL database to use when connecting to the PostgreSQL database server.
         */
        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        /** Password to use when connecting to the PostgreSQL database server. */
        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        /**
         * The name of the PostgreSQL logical decoding slot that was created for streaming changes
         * from a particular plug-in for a particular database/schema. The server uses this slot to
         * stream events to the connector that you are configuring.
         *
         * <p>Slot names must conform to <a
         * href="https://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">PostgreSQL
         * replication slot naming rules</a>, which state: "Each replication slot has a name, which
         * can contain lower-case letters, numbers, and the underscore character."
         */
        public Builder<T> slotName(String slotName) {
            this.slotName = slotName;
            return this;
        }

        /** The Debezium Postgres connector properties. */
        public Builder<T> debeziumProperties(Properties properties) {
            this.dbzProperties = properties;
            return this;
        }

        public Builder<T> heartbeatMs(String defaultHeartbeatMs) {
            this.defaultHeartbeatMs = defaultHeartbeatMs;
            return this;
        }

        /**
         * The deserializer used to convert from consumed {@link
         * org.apache.kafka.connect.source.SourceRecord}.
         */
        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public DebeziumSourceFunction<T> build() {
            Properties props = new Properties();
            props.setProperty("connector.class", PostgresConnector.class.getCanonicalName());
            props.setProperty("plugin.name", pluginName);
            props.setProperty("database.hostname", checkNotNull(hostname));
            props.setProperty("database.dbname", checkNotNull(database));
            props.setProperty("database.user", checkNotNull(username));
            props.setProperty("database.password", checkNotNull(password));
            props.setProperty("database.port", String.valueOf(port));
            props.setProperty("slot.name", slotName);
            props.setProperty("slot.drop.on.stop", dropEnable);
            props.setProperty("database.server.name", UUID.randomUUID().toString());

            // we have to enable heartbeat for PG to make sure DebeziumChangeConsumer#handleBatch
            // is invoked after job restart
            props.setProperty("heartbeat.interval.ms", defaultHeartbeatMs);

            if (schemaList != null) {
                props.setProperty("schema.include.list", String.join(",", schemaList));
            }
            if (tableList != null) {
                props.setProperty("table.include.list", String.join(",", tableList));
            }

            if (dbzProperties != null) {
                props.putAll(dbzProperties);
            }

            return new PgsqlDebeziumSourceFunction<>(
                    deserializer,
                    props,
                    null,
                    Validator.getDefaultValidator(),
                    tableList,
                    sourceConfig);
        }
    }
}
