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

package com.ververica.cdc.cli;

import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import com.ververica.cdc.cli.application.YarnApplicationOptions;
import com.ververica.cdc.cli.parser.PipelineDefinitionParser;
import com.ververica.cdc.cli.parser.YamlPipelineDefinitionParser;
import com.ververica.cdc.cli.utils.FlinkEnvironmentUtils;
import com.ververica.cdc.common.annotation.VisibleForTesting;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import com.ververica.cdc.composer.PipelineComposer;
import com.ververica.cdc.composer.PipelineExecution;
import com.ververica.cdc.composer.definition.PipelineDef;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.ververica.cdc.cli.CliFrontend.addShipFile;

/** Executor for doing the composing and submitting logic for {@link CliFrontend}. */
public class CliExecutor {

    private final Path pipelineDefPath;
    private final Configuration flinkConfig;
    private final Configuration globalPipelineConfig;
    private final boolean useMiniCluster;
    private final List<Path> additionalJars;

    private PipelineComposer composer = null;

    private final SavepointRestoreSettings savepointSettings;

    private final boolean useApplicationMode;
    private final ApplicationConfiguration applicationConfiguration;

    private final Configuration cliConfiguration;
    private static final String FLINK_CONF_PREFIX = "flink-conf.";

    public CliExecutor(
            Path pipelineDefPath,
            Configuration flinkConfig,
            Configuration globalPipelineConfig,
            boolean useMiniCluster,
            List<Path> additionalJars,
            SavepointRestoreSettings savepointSettings) {
        this(
                pipelineDefPath,
                flinkConfig,
                globalPipelineConfig,
                useMiniCluster,
                additionalJars,
                savepointSettings,
                false,
                null,
                new Configuration());
    }

    public CliExecutor(
            Path pipelineDefPath,
            Configuration flinkConfig,
            Configuration globalPipelineConfig,
            boolean useMiniCluster,
            List<Path> additionalJars,
            SavepointRestoreSettings savepointSettings,
            boolean useApplicationMode,
            ApplicationConfiguration applicationConfiguration,
            Configuration cliConfiguration) {
        this.pipelineDefPath = pipelineDefPath;
        this.flinkConfig = flinkConfig;
        this.globalPipelineConfig = globalPipelineConfig;
        this.useMiniCluster = useMiniCluster;
        this.additionalJars = additionalJars;
        this.savepointSettings = savepointSettings;
        this.useApplicationMode = useApplicationMode;
        this.applicationConfiguration = applicationConfiguration;
        this.cliConfiguration = cliConfiguration;
    }

    public PipelineExecution.ExecutionInfo run() throws Exception {
        // Parse pipeline definition file
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                pipelineDefinitionParser.parse(pipelineDefPath, globalPipelineConfig);

        // Add Flink configuration from pipeline definition to flinkConfig
        addFlinkConfigurationFromPipelineDef(flinkConfig, pipelineDef);
        // Add Flink configuration from cli to flinkConfig
        flinkConfig.addAll(cliConfiguration);

        // If run the pipeline in application mode, submit an application for execution in
        // "Application Mode" with configurations
        if (useApplicationMode) {
            String clusterId = submitApplication(pipelineDef);
            return new PipelineExecution.ExecutionInfo(clusterId, "Application submitted");
        }

        // Create composer
        PipelineComposer composer = getComposer();

        // Compose pipeline
        PipelineExecution execution = composer.compose(pipelineDef);

        // Execute the pipeline
        return execution.execute();
    }

    private PipelineComposer getComposer() {
        if (composer == null) {
            return FlinkEnvironmentUtils.createComposer(
                    useMiniCluster, flinkConfig, additionalJars, savepointSettings);
        }
        return composer;
    }

    /**
     * submit an flink application for execution in "Application Mode" with configurations.
     *
     * @param pipelineDef
     * @return clusterId
     * @throws Exception
     */
    private String submitApplication(PipelineDef pipelineDef) throws Exception {
        ClusterClientServiceLoader clusterClientServiceLoader =
                new DefaultClusterClientServiceLoader();
        ApplicationClusterDeployer deployer =
                new ApplicationClusterDeployer(clusterClientServiceLoader);

        // If the pipeline definition file is encrypted and requires private key for decryption,
        // ship the private key file to the YARN cluster
        if (pipelineDef.getIsEncrypted()
                && pipelineDef
                        .getConfig()
                        .contains(PipelineOptions.ENCRYPTOR_PRIVATE_KEY_LOCATION)) {
            String privateKeyFile =
                    pipelineDef.getConfig().get(PipelineOptions.ENCRYPTOR_PRIVATE_KEY_LOCATION);
            addShipFile(privateKeyFile, flinkConfig);
        }

        // convert configuration from flinkcdc to flink configuration
        org.apache.flink.configuration.Configuration flinkApplicationConf =
                org.apache.flink.configuration.Configuration.fromMap(flinkConfig.toMap());
        SavepointRestoreSettings.toConfiguration(savepointSettings, flinkApplicationConf);

        // set log directory where log4j.properties is located
        flinkApplicationConf.set(
                DeploymentOptionsInternal.CONF_DIR,
                flinkConfig.get(YarnApplicationOptions.APPLICATION_LOG_CONFIG_DIR));

        // set flinkcdc-dist jar as pipeline.jars
        String jarName =
                CliFrontend.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation()
                        .toURI()
                        .toString();
        flinkApplicationConf.set(
                org.apache.flink.configuration.PipelineOptions.JARS,
                Collections.singletonList(jarName));

        // run application
        deployer.run(flinkApplicationConf, applicationConfiguration);

        return Objects.requireNonNull(
                        clusterClientServiceLoader
                                .getClusterClientFactory(flinkApplicationConf)
                                .getClusterId(flinkApplicationConf))
                .toString();
    }

    private void addFlinkConfigurationFromPipelineDef(
            Configuration flinkConfig, PipelineDef pipelineDef) {
        Map<String, String> pipelineFlinkConfigs = pipelineDef.getConfig().toMap();
        if (pipelineFlinkConfigs.containsKey(PipelineOptions.PIPELINE_NAME.key())) {
            flinkConfig.set(
                    YarnApplicationOptions.APPLICATION_NAME,
                    pipelineFlinkConfigs.get(PipelineOptions.PIPELINE_NAME.key()));
        }
        Map<String, String> flinkConfToAdd =
                pipelineFlinkConfigs.entrySet().stream()
                        .filter(
                                pipelineConfig ->
                                        pipelineConfig.getKey().startsWith(FLINK_CONF_PREFIX))
                        .collect(
                                Collectors.toMap(
                                        pipelineConfig ->
                                                pipelineConfig
                                                        .getKey()
                                                        .substring(FLINK_CONF_PREFIX.length()),
                                        Map.Entry::getValue));
        Configuration newFlinkConf = Configuration.fromMap(flinkConfToAdd);
        flinkConfig.addAll(newFlinkConf);
    }

    @VisibleForTesting
    void setComposer(PipelineComposer composer) {
        this.composer = composer;
    }

    @VisibleForTesting
    public Configuration getFlinkConfig() {
        return flinkConfig;
    }

    @VisibleForTesting
    public Configuration getGlobalPipelineConfig() {
        return globalPipelineConfig;
    }

    @VisibleForTesting
    public Configuration getCliConfiguration() {
        return cliConfiguration;
    }

    @VisibleForTesting
    public List<Path> getAdditionalJars() {
        return additionalJars;
    }

    public SavepointRestoreSettings getSavepointSettings() {
        return savepointSettings;
    }
}
