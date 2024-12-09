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

package com.ververica.cdc.common.shade;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The interface that provides the ability to decrypt {@link com.ververica.cdc.composer.definition}.
 */
@PublicEvolving
public interface ConfigShade {
    Logger LOG = LoggerFactory.getLogger(ConfigShade.class);

    default void initialize(Configuration pipelineConfig) throws Exception {}

    /**
     * The unique identifier of the current interface, used it to select the correct {@link
     * ConfigShade}.
     */
    String getIdentifier();

    /**
     * Decrypt the content.
     *
     * @param content The content to decrypt
     */
    String decrypt(String content);

    /**
     * Add a ship file path to the configuration.
     *
     * <p>This method is used to add file paths to the {@link
     * com.ververica.cdc.common.pipeline.PipelineOptions#ADDITIONAL_SHIP_FILES}. In the
     * yarn-application mode, these files will be shipped to the cluster and added to the classpath
     * resources on the master. If an implementation of ConfigShade requires an additional file
     * (e.g., a key file), it should call this method to upload that file to support decryption in
     * application mode.
     *
     * @param filePath The file path to be added.
     * @param pipelineConfig The pipeline configuration to store the additional ship file paths.
     */
    static void addShipFile(String filePath, Configuration pipelineConfig) {
        List<String> shipFiles =
                pipelineConfig.contains(PipelineOptions.ADDITIONAL_SHIP_FILES)
                        ? pipelineConfig.get(PipelineOptions.ADDITIONAL_SHIP_FILES)
                        : new ArrayList<>();
        shipFiles.add(filePath);
        pipelineConfig.set(PipelineOptions.ADDITIONAL_SHIP_FILES, shipFiles);
        LOG.info("Set additional files for decryption {}", shipFiles);
    }

    /**
     * Add a key-value pair to the flink configuration.
     *
     * <p>This method is used to add a config to the Flink Configurations. In the yarn-application
     * mode, this config can be read from the configurations of {@link
     * org.apache.flink.streaming.api.environment.StreamExecutionEnvironment} on the master. If an
     * implementation of ConfigShade requires an additional file (e.g., a key file), then the file
     * contents can be added to the flink configuration to support reading on the master side in
     * application mode.
     *
     * @param key The configuration key to be added.
     * @param value The configuration value to be added.
     * @param pipelineConfig The pipeline configuration to store the additional ship file paths.
     */
    static void addFlinkConfiguration(String key, String value, Configuration pipelineConfig) {
        Map<String, String> additionalFlinkConf =
                pipelineConfig.contains(PipelineOptions.ADDITIONAL_FLINK_CONF)
                        ? pipelineConfig.get(PipelineOptions.ADDITIONAL_FLINK_CONF)
                        : new HashMap<>();
        additionalFlinkConf.put(key, value);
        pipelineConfig.set(PipelineOptions.ADDITIONAL_FLINK_CONF, additionalFlinkConf);
        LOG.info("Set additional Flink configuration {} for decryption", key);
    }
}
