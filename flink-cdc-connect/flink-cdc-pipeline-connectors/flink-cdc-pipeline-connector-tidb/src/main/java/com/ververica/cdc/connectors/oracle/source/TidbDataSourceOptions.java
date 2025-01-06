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

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.ConfigOptions;

/** Configurations for {@link TidbDataSource}. */
@PublicEvolving
public class TidbDataSourceOptions {

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the oracle database server.");

    public static final ConfigOption<Integer> PD_PORT =
            ConfigOptions.key("pd.port")
                    .intType()
                    .defaultValue(2379)
                    .withDescription("Integer port number of the oracle database server.");

    public static final ConfigOption<Integer> TIDB_PORT =
            ConfigOptions.key("tidb.port")
                    .intType()
                    .defaultValue(4000)
                    .withDescription("Integer port number of the oracle database server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the oracle database to use when connecting to the oracle database server.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to use when connecting to the oracle database server.");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to use when connecting to the oracle database server.");
    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("tables")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table names of the oracle tables to monitor. Regular expressions are supported. "
                                    + "It is important to note that the dot (.) is treated as a delimiter for database and table names. "
                                    + "If there is a need to use a dot (.) in a regular expression to match any character, "
                                    + "it is necessary to escape the dot with a backslash."
                                    + "eg. db0.\\.*, db1.user_table_[0-9]+, db[1-2].[app|web]_order_\\.*");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The session time zone in database server. If not set, then "
                                    + "ZoneId.systemDefault() is used to determine the server time zone.");

    public static final ConfigOption<String> TLS_KEY_FILE =
            ConfigOptions.key("tls.keyfile")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum fetch size for per poll when read table snapshot.");

    public static final ConfigOption<String> TLS_CERT_FILE =
            ConfigOptions.key("tls.certfile")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum time that the connector should wait after trying to connect to the oracle database server before timing out.");

    public static final ConfigOption<String> TLS_CHAINFILE =
            ConfigOptions.key("tls.chainfile")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The connection pool size.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for oracle CDC consumer, valid enumerations are "
                                    + "\"initial\", \"earliest-offset\", \"latest-offset\", \"timestamp\"\n"
                                    + "or \"specific-offset\"");
}
