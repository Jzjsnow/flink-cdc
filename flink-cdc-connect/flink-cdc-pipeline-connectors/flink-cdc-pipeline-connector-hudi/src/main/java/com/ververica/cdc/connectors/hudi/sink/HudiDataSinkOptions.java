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

package com.ververica.cdc.connectors.hudi.sink;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.ConfigOptions;
import com.ververica.cdc.common.configuration.Configuration;
import org.apache.hudi.common.model.WriteOperationType;

import java.util.HashMap;
import java.util.Map;

/** HudiDataSink Options reference {@link HudiDataSinkOptions}. */
public class HudiDataSinkOptions {
    public static final ConfigOption<String> TABLENAME =
            ConfigOptions.key("tablename")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("tablename.");
    public static final ConfigOption<String> TABLEPATH =
            ConfigOptions.key("option.pipeline.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("table hdfs path.");
    public static final ConfigOption<String> PRIMARYKEY =
            ConfigOptions.key("pk")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("table primary key.");

    public static final ConfigOption<String> PARTITIONFIELD =
            ConfigOptions.key("partition.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("table partition.field.");

    public static final ConfigOption<String> WRITE_OPERATION_MODE =
            ConfigOptions.key("option.pipeline.write.operation")
                    .stringType()
                    .defaultValue(WriteOperationType.UPSERT.value())
                    .withDescription("The write operation, that this write should do");

    public static final ConfigOption<Integer> READ_STREAMING_CHECK_INTERVAL =
            ConfigOptions.key("option.pipeline.read.streaming.check-interval")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Check interval for streaming read of SECOND, default 1 minute.");
    public static final ConfigOption<Boolean> READ_AS_STREAMING =
            ConfigOptions.key("option.pipeline.read.streaming.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to read as streaming source, default false.");
    public static final ConfigOption<Boolean> HIVE_SYNC_ENABLED =
            ConfigOptions.key("option.pipeline.hive_sync.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Asynchronously sync Hive meta to HMS, default false.");
    public static final ConfigOption<Boolean> DATASOURCE_WRITE_RECONCILE_SCHEMA =
            ConfigOptions.key("option.pipeline.hoodie.datasource.write.reconcile.schema")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "In the context of Hudi, schema reconciliation means that when writing data, Hudi will try to match and reconcile the source data schema with the Hudi table  current schema to ensure data correctness and compatibility.");
    public static final ConfigOption<Boolean> SCHEMA_ON_READ_ENABLE =
            ConfigOptions.key("option.pipeline.hoodie.schema.on.read.enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "In the context of Hudi, schema reconciliation means that when writing data, Hudi will try to match and reconcile the source data schema with the Hudi table  current schema to ensure data correctness and compatibility.");
    public static final ConfigOption<Integer> CLEANER_COMMITS_RETAINED =
            ConfigOptions.key("option.pipeline.hoodie.cleaner.commits.retained")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "It is used to control the number of latest commits that Hudi retains when performing a clean operation.");
    public static final ConfigOption<String> INDEX_TYPE =
            ConfigOptions.key("option.pipeline.hoodie.index.type")
                    .stringType()
                    .defaultValue("GLOBAL_SIMPLE")
                    .withDescription("hoodie index type");
    public static final ConfigOption<Integer> WRITE_TASKS_NUM =
            ConfigOptions.key("option.pipeline.write.tasks")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Parallelism of tasks that do actual write, default is the parallelism of the execution environment");
    public static final ConfigOption<Integer> COMPACTION_TASKS_NUM =
            ConfigOptions.key("option.pipeline.hoodie.compaction.tasks")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Parallelism of tasks that do actual compaction, default same as the write task parallelism");
    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server.time.zone")
                    .stringType()
                    .defaultValue("Etc/GMT-8")
                    .withDescription("hudi time zone.");
    public static final ConfigOption<String> HDFS_CACHE =
            ConfigOptions.key("option.hadoop.hdfs.fs.hdfs.impl.disable.cache")
                    .stringType()
                    .defaultValue("true")
                    .withDescription("if open hdfs cache.");

    public static final ConfigOption<String> USE_DATANODE_HOSTNAME =
            ConfigOptions.key("option.hadoop.dfs.client.use.datanode.hostname")
                    .stringType()
                    .defaultValue("true")
                    .withDescription(
                            "HDFS clients attempt to establish connections using the IP addresses of data nodes. However, in some network environments, it may be desirable or necessary for clients to establish connections using the hostnames of data nodes.");

    public static final ConfigOption<String> KERBEROS_PRINCIPAL_PATTERN =
            ConfigOptions.key("option.hadoop.dfs.namenode.kerberos.principal.pattern")
                    .stringType()
                    .defaultValue("true")
                    .withDescription(
                            "Set the matching mode of the Kerberos principal of the NameNode service to allow authentication for all realms.");

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
