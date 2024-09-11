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

package com.ververica.cdc.connectors.iceberg.sink;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.ConfigOptions;
import com.ververica.cdc.common.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

/** IcebergDataSink Options reference {@link IcebergDataSinkOptions}. */
public class IcebergDataSinkOptions {
    public static final ConfigOption<String> CATALOG_TYPE =
            ConfigOptions.key("catalog.type")
                    .stringType()
                    .defaultValue("hive")
                    .withDescription("The type of catalog. eg. hive,hadoop,rest");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("sink write data parallelism.");
    public static final ConfigOption<Boolean> UPSERT_MODE =
            ConfigOptions.key("upsert.mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("If open the data write upsert mode");
    public static final ConfigOption<Boolean> OVERWRITE_MODE =
            ConfigOptions.key("overwrite.mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("sink overwrite mode.");
    public static final ConfigOption<String> CATALOG_NAME =
            ConfigOptions.key("catalog.name")
                    .stringType()
                    .defaultValue("default_catalog")
                    .withDescription("The catalog name of catalog.");
    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The database of the table to be written.");
    public static final ConfigOption<String> TABLENAME =
            ConfigOptions.key("tablename")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the table to be written.");
    public static final ConfigOption<String> WAREHOUSE =
            ConfigOptions.key("catalog.hive.warehouse")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Hive warehouse location, users should specify this path if neither set the hive-conf-dir to specify a location containing a hive-site.xml configuration file nor add a correct hive-site.xml to classpath.");
    public static final ConfigOption<String> HIVEURI =
            ConfigOptions.key("catalog.hive.uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Hive metastore's thrift URI.");

    public static final ConfigOption<String> FSDEFAULTFS =
            ConfigOptions.key("catalog.hdfs.fs.defaultFs")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the default file system .");
    public static final ConfigOption<Boolean> HDFS_CACHE =
            ConfigOptions.key("catalog.hdfs.fs.hdfs.impl.disable.cache")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("if open hdfs cache.");

    public static final ConfigOption<String> CLIENT_POOL_SIZE =
            ConfigOptions.key("catalog.hive.clients")
                    .stringType()
                    .defaultValue("5")
                    .withDescription("The Hive metastore client pool size, default value is 2. ");

    public static Map<String, String> getPropertiesByPrefix(
            Configuration tableOptions, String prefix) {
        final Map<String, String> props = new HashMap<>();
        for (Map.Entry<String, String> entry : tableOptions.toMap().entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String subKey = entry.getKey().substring(prefix.length());
                props.put(subKey, entry.getValue());
            }
        }
        return props;
    }
}
