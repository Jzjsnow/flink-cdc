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

package com.ververica.cdc.common.pipeline;

import org.apache.flink.configuration.description.TextElement;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.ConfigOptions;
import com.ververica.cdc.common.configuration.description.Description;
import com.ververica.cdc.common.configuration.description.ListElement;

import java.time.Duration;

import static com.ververica.cdc.common.configuration.description.TextElement.text;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS;

/** Predefined pipeline configuration options. */
@PublicEvolving
public class PipelineOptions {
    public static final Duration DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT = Duration.ofMinutes(3);
    public static final ConfigOption<String> PIPELINE_NAME =
            ConfigOptions.key("name")
                    .stringType()
                    .defaultValue("Flink CDC Pipeline Job")
                    .withDescription("The name of the pipeline");

    public static final ConfigOption<Integer> PIPELINE_PARALLELISM =
            ConfigOptions.key("parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Parallelism of the pipeline");

    public static final ConfigOption<SchemaChangeBehavior> PIPELINE_SCHEMA_CHANGE_BEHAVIOR =
            ConfigOptions.key("schema.change.behavior")
                    .enumType(SchemaChangeBehavior.class)
                    .defaultValue(SchemaChangeBehavior.EVOLVE)
                    .withDescription(
                            Description.builder()
                                    .text("Behavior for handling schema change events. ")
                                    .linebreak()
                                    .add(
                                            ListElement.list(
                                                    text(
                                                            "EVOLVE: Apply schema changes to downstream. This requires sink to support handling schema changes."),
                                                    text("IGNORE: Drop all schema change events."),
                                                    text(
                                                            "EXCEPTION: Throw an exception to terminate the sync pipeline.")))
                                    .build());

    public static final ConfigOption<String> PIPELINE_LOCAL_TIME_ZONE =
            ConfigOptions.key("local-time-zone")
                    .stringType()
                    // "systemDefault" is a special value to decide whether to use
                    // ZoneId.systemDefault() in
                    // PipelineOptions.getLocalTimeZone()
                    .defaultValue("systemDefault")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The local time zone defines current session time zone id. ")
                                    .linebreak()
                                    .text(
                                            "It is used when converting to/from <code>TIMESTAMP WITH LOCAL TIME ZONE</code>. "
                                                    + "Internally, timestamps with local time zone are always represented in the UTC time zone. "
                                                    + "However, when converting to data types that don't include a time zone (e.g. TIMESTAMP, STRING), "
                                                    + "the session time zone is used during conversion. The input of option is either a full name "
                                                    + "such as \"America/Los_Angeles\", or a custom timezone id such as \"GMT-08:00\".")
                                    .build());

    public static final ConfigOption<String> PIPELINE_SCHEMA_OPERATOR_UID =
            ConfigOptions.key("schema.operator.uid")
                    .stringType()
                    .defaultValue("$$_schema_operator_$$")
                    .withDescription(
                            "The unique ID for schema operator. This ID will be used for inter-operator communications and must be unique across operators.");

    public static final ConfigOption<Boolean> ENCRYPTOR_ENABLED =
            ConfigOptions.key("jasypt-encryptor.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable encryptor.");

    public static final ConfigOption<String> ENCRYPTOR_PRIVATE_KEY_FORMAT =
            ConfigOptions.key("jasypt-encryptor.private-key-format")
                    .stringType()
                    .defaultValue("DER")
                    .withDescription("The format of private key, DER or PEM.");

    public static final ConfigOption<String> ENCRYPTOR_PRIVATE_KEY_STRING =
            ConfigOptions.key("jasypt-encryptor.private-key-string")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The private key for decryption in String format.");

    public static final ConfigOption<String> ENCRYPTOR_PRIVATE_KEY_LOCATION =
            ConfigOptions.key("jasypt-encryptor.private-key-location")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The location of the private key for decryption.");

    public static final ConfigOption<Duration> CHECKPOINTING_TIMEOUT =
            ConfigOptions.key("execution.checkpointing.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "The maximum time that a checkpoint may take before being discarded.");

    public static final ConfigOption<Duration> CHECKPOINTING_INTERVAL =
            ConfigOptions.key("execution.checkpointing.interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            org.apache.flink.configuration.description.Description.builder()
                                    .text(
                                            "Gets the interval in which checkpoints are periodically scheduled.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "This setting defines the base interval. Checkpoint triggering may be delayed by the settings "
                                                    + "%s and %s",
                                            TextElement.code(MAX_CONCURRENT_CHECKPOINTS.key()),
                                            TextElement.code(MIN_PAUSE_BETWEEN_CHECKPOINTS.key()))
                                    .build()
                                    .toString());
    public static final ConfigOption<Duration> PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT =
            ConfigOptions.key("schema-operator.rpc-timeout")
                    .durationType()
                    .defaultValue(DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT)
                    .withDescription(
                            "The timeout time for SchemaOperator to wait downstream SchemaChangeEvent applying finished, the default value is 3 minutes.");

    private PipelineOptions() {}
}
