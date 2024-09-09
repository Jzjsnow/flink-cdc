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

package com.ververica.cdc.cli.utils;

import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.cli.application.YarnDeploymentTargetEnum;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.composer.flink.FlinkPipelineComposer;

import java.nio.file.Path;
import java.util.List;

/** Utilities for handling Flink configuration and environment. */
public class FlinkEnvironmentUtils {

    private static final String FLINK_CONF_DIR = "conf";
    private static final String FLINK_CONF_FILENAME = "flink-conf.yaml";

    public static Configuration loadFlinkConfiguration(Path flinkHome) throws Exception {
        Path flinkConfPath = getFlinkConfigurationDir(flinkHome).resolve(FLINK_CONF_FILENAME);
        return ConfigurationUtils.loadMapFormattedConfig(flinkConfPath);
    }

    public static Path getFlinkConfigurationDir(Path flinkHome) {
        return flinkHome.resolve(FLINK_CONF_DIR);
    }

    public static boolean checkIfRunningOnApplicationMaster() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (YarnDeploymentTargetEnum.EMBEDDED
                .getName()
                .equalsIgnoreCase(env.getConfiguration().get(DeploymentOptions.TARGET))) {
            return true;
        }
        return false;
    }

    public static FlinkPipelineComposer createComposer(
            boolean useMiniCluster,
            Configuration flinkConfig,
            List<Path> additionalJars,
            SavepointRestoreSettings savepointSettings) {
        if (checkIfRunningOnApplicationMaster()) {
            return FlinkPipelineComposer.ofMiniCluster();
        }
        if (useMiniCluster) {
            return FlinkPipelineComposer.ofMiniCluster();
        }
        org.apache.flink.configuration.Configuration configuration =
                org.apache.flink.configuration.Configuration.fromMap(flinkConfig.toMap());
        SavepointRestoreSettings.toConfiguration(savepointSettings, configuration);
        return FlinkPipelineComposer.ofRemoteCluster(configuration, additionalJars);
    }
}
