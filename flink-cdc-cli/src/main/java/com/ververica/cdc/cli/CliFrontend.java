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

import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import com.ververica.cdc.cli.application.YarnApplicationOptions;
import com.ververica.cdc.cli.application.YarnDeploymentTargetEnum;
import com.ververica.cdc.cli.utils.ConfigurationUtils;
import com.ververica.cdc.cli.utils.FlinkEnvironmentUtils;
import com.ververica.cdc.common.annotation.VisibleForTesting;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.composer.PipelineExecution;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.ververica.cdc.cli.CliFrontendOptions.SAVEPOINT_ALLOW_NON_RESTORED_OPTION;
import static com.ververica.cdc.cli.CliFrontendOptions.SAVEPOINT_CLAIM_MODE;
import static com.ververica.cdc.cli.CliFrontendOptions.SAVEPOINT_PATH_OPTION;
import static com.ververica.cdc.cli.utils.ConfigurationUtils.filterProperties;
import static com.ververica.cdc.cli.utils.ConfigurationUtils.getCliConfiguration;

/** The frontend entrypoint for the command-line interface of Flink CDC. */
public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);
    private static final String FLINK_HOME_ENV_VAR = "FLINK_HOME";
    private static final String FLINK_CDC_HOME_ENV_VAR = "FLINK_CDC_HOME";

    public static void main(String[] args) throws Exception {
        Options cliOptions = CliFrontendOptions.initializeOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(cliOptions, filterProperties(args));

        // Help message
        if (args.length == 0 || commandLine.hasOption(CliFrontendOptions.HELP)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setLeftPadding(4);
            formatter.setWidth(80);
            formatter.printHelp(" ", cliOptions);
            return;
        }

        // Create executor and execute the pipeline
        PipelineExecution.ExecutionInfo result = createExecutor(commandLine).run();

        // Print execution result
        printExecutionInfo(result);
    }

    @VisibleForTesting
    static CliExecutor createExecutor(CommandLine commandLine) throws Exception {
        // The pipeline definition file would remain unparsed
        List<String> unparsedArgs = commandLine.getArgList();
        if (unparsedArgs.isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing pipeline definition file path in arguments. ");
        }

        // Take the first unparsed argument as the pipeline definition file
        Path pipelineDefPath = Paths.get(unparsedArgs.get(0));
        if (!Files.exists(pipelineDefPath)) {
            throw new FileNotFoundException(
                    String.format("Cannot find pipeline definition file \"%s\"", pipelineDefPath));
        }

        Configuration cliConfiguration = getCliConfiguration(unparsedArgs);

        // Global pipeline configuration
        Configuration globalPipelineConfig = getGlobalConfig(commandLine);

        // Check if running on the cluster in application mode
        if (FlinkEnvironmentUtils.checkIfRunningOnApplicationMaster()) {
            // When running on the master, the flink configurations and save point settings are
            // already stored in the
            // StreamExecutionEnvironment, so passing a null value when constructing CliExecutor is
            // sufficient
            LOG.info("Running on cluster, skip loading Flink configurations.");

            // Build executor
            return new CliExecutor(
                    pipelineDefPath,
                    new Configuration(),
                    globalPipelineConfig,
                    commandLine.hasOption(CliFrontendOptions.USE_MINI_CLUSTER),
                    new ArrayList<>(),
                    null);
        }
        // Load Flink environment
        Path flinkHome = getFlinkHome(commandLine);
        Configuration flinkConfig = FlinkEnvironmentUtils.loadFlinkConfiguration(flinkHome);

        // Savepoint
        SavepointRestoreSettings savepointSettings = createSavepointRestoreSettings(commandLine);

        // Additional JARs
        List<Path> additionalJars =
                Arrays.stream(
                                Optional.ofNullable(
                                                commandLine.getOptionValues(CliFrontendOptions.JAR))
                                        .orElse(new String[0]))
                        .map(Paths::get)
                        .collect(Collectors.toList());

        // Check if going to run the pipeline in application mode
        boolean useApplicationMode =
                commandLine.hasOption(CliFrontendOptions.USE_YARN_APPLICATION_MODE)
                        || YarnDeploymentTargetEnum.APPLICATION
                                .getName()
                                .equalsIgnoreCase(flinkConfig.get(YarnApplicationOptions.TARGET));
        LOG.info("Use application mode: " + useApplicationMode);

        if (useApplicationMode) {
            flinkConfig.set(
                    YarnApplicationOptions.TARGET, YarnDeploymentTargetEnum.APPLICATION.getName());
            final ApplicationConfiguration applicationConfiguration =
                    createApplicationConfiguration(
                            commandLine, pipelineDefPath, additionalJars, flinkHome, flinkConfig);
            return new CliExecutor(
                    pipelineDefPath,
                    flinkConfig,
                    globalPipelineConfig,
                    commandLine.hasOption(CliFrontendOptions.USE_MINI_CLUSTER),
                    additionalJars,
                    savepointSettings,
                    true,
                    applicationConfiguration,
                    cliConfiguration);
        }
        // Build executor
        return new CliExecutor(
                pipelineDefPath,
                flinkConfig,
                globalPipelineConfig,
                commandLine.hasOption(CliFrontendOptions.USE_MINI_CLUSTER),
                additionalJars,
                savepointSettings,
                false,
                null,
                cliConfiguration);
    }

    /** create a CliExecutor to submit a Flink application on the client. */
    private static ApplicationConfiguration createApplicationConfiguration(
            CommandLine commandLine,
            Path pipelineDefPath,
            List<Path> additionalJars,
            Path flinkHome,
            Configuration flinkConfig) {
        // running on the client and submitting a Flink application

        String pipelineFileName = pipelineDefPath.toFile().getName();
        Path globalConfigPath = getGlobalConfigPath(commandLine);

        // create the list of files and/or directories to be shipped to the YARN cluster
        addShipFile(pipelineDefPath.toString(), flinkConfig);
        addShipFiles(
                additionalJars.stream().map(Path::toString).collect(Collectors.toList()),
                flinkConfig);
        if (Objects.nonNull(globalConfigPath)) {
            addShipFile(globalConfigPath.toString(), flinkConfig);
        }

        // reorganize the program arguments for application runs on the cluster
        List<String> applicationArgs = new ArrayList<>();
        applicationArgs.add(pipelineFileName);
        if (Objects.nonNull(globalConfigPath)) {
            applicationArgs.add(
                    "--"
                            + CliFrontendOptions.GLOBAL_CONFIG.getLongOpt()
                            + "="
                            + globalConfigPath.getFileName());
        }

        String[] programArguments = applicationArgs.toArray(new String[0]);
        LOG.info("Arguments to run program on the cluster: " + Arrays.toString(programArguments));
        final ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(programArguments, YarnApplicationOptions.CLASS_NAME);

        Path flinkConfHome = FlinkEnvironmentUtils.getFlinkConfigurationDir(flinkHome);
        flinkConfig.set(
                YarnApplicationOptions.APPLICATION_LOG_CONFIG_DIR, flinkConfHome.toString());

        return applicationConfiguration;
    }

    public static void addShipFile(String fileToShip, Configuration flinkConfig) {
        List<String> shipFiles =
                flinkConfig.contains(YarnApplicationOptions.SHIP_FILES)
                        ? flinkConfig.get(YarnApplicationOptions.SHIP_FILES)
                        : new ArrayList<>();
        shipFiles.add(fileToShip);
        flinkConfig.set(YarnApplicationOptions.SHIP_FILES, shipFiles);
        LOG.info("Add File {} to be shipped to the YARN cluster: ", fileToShip);
    }

    public static void addShipFiles(List<String> filesToShip, Configuration flinkConfig) {
        if (filesToShip.isEmpty()) {
            return;
        }
        List<String> shipFiles =
                flinkConfig.contains(YarnApplicationOptions.SHIP_FILES)
                        ? flinkConfig.get(YarnApplicationOptions.SHIP_FILES)
                        : new ArrayList<>();
        shipFiles.addAll(filesToShip);
        flinkConfig.set(YarnApplicationOptions.SHIP_FILES, shipFiles);
        LOG.info("Add Files {} to be shipped to the YARN cluster: ", filesToShip);
    }

    private static SavepointRestoreSettings createSavepointRestoreSettings(
            CommandLine commandLine) {
        if (commandLine.hasOption(SAVEPOINT_PATH_OPTION.getOpt())) {
            String savepointPath = commandLine.getOptionValue(SAVEPOINT_PATH_OPTION.getOpt());
            boolean allowNonRestoredState =
                    commandLine.hasOption(SAVEPOINT_ALLOW_NON_RESTORED_OPTION.getOpt());
            final RestoreMode restoreMode;
            if (commandLine.hasOption(SAVEPOINT_CLAIM_MODE)) {
                restoreMode =
                        org.apache.flink.configuration.ConfigurationUtils.convertValue(
                                commandLine.getOptionValue(SAVEPOINT_CLAIM_MODE),
                                RestoreMode.class);
            } else {
                restoreMode = SavepointConfigOptions.RESTORE_MODE.defaultValue();
            }
            // allowNonRestoredState is always false because all operators are predefined.
            return SavepointRestoreSettings.forPath(
                    savepointPath, allowNonRestoredState, restoreMode);
        } else {
            return SavepointRestoreSettings.none();
        }
    }

    private static Path getFlinkHome(CommandLine commandLine) {
        // Check command line arguments first
        String flinkHomeFromArgs = commandLine.getOptionValue(CliFrontendOptions.FLINK_HOME);
        if (flinkHomeFromArgs != null) {
            LOG.debug("Flink home is loaded by command-line argument: {}", flinkHomeFromArgs);
            return Paths.get(flinkHomeFromArgs);
        }

        // Fallback to environment variable
        String flinkHomeFromEnvVar = System.getenv(FLINK_HOME_ENV_VAR);
        if (flinkHomeFromEnvVar != null) {
            LOG.debug("Flink home is loaded by environment variable: {}", flinkHomeFromEnvVar);
            return Paths.get(flinkHomeFromEnvVar);
        }

        throw new IllegalArgumentException(
                "Cannot find Flink home from either command line arguments \"--flink-home\" "
                        + "or the environment variable \"FLINK_HOME\". "
                        + "Please make sure Flink home is properly set. ");
    }

    private static Configuration getGlobalConfig(CommandLine commandLine) throws Exception {
        // Try to get global config path from command line
        Path globalConfigPath = getGlobalConfigPath(commandLine);
        if (globalConfigPath != null) {
            return ConfigurationUtils.loadMapFormattedConfig(globalConfigPath);
        }

        // Fallback to empty configuration
        LOG.warn(
                "Cannot find global configuration in command-line or FLINK_CDC_HOME. Will use empty global configuration.");
        return new Configuration();
    }

    private static Path getGlobalConfigPath(CommandLine commandLine) {
        // Try to get global config path from command line
        String globalConfig = commandLine.getOptionValue(CliFrontendOptions.GLOBAL_CONFIG);
        if (globalConfig != null) {
            Path globalConfigPath = Paths.get(globalConfig);
            LOG.info("Using global config in command line: {}", globalConfigPath);
            return globalConfigPath;
        }

        // Fallback to Flink CDC home
        String flinkCdcHome = System.getenv(FLINK_CDC_HOME_ENV_VAR);
        if (flinkCdcHome != null) {
            Path globalConfigPath =
                    Paths.get(flinkCdcHome).resolve("conf").resolve("flink-cdc.yaml");
            LOG.info("Using global config in FLINK_CDC_HOME: {}", globalConfigPath);
            return globalConfigPath;
        }

        // Fallback to empty configuration
        return null;
    }

    private static void printExecutionInfo(PipelineExecution.ExecutionInfo info) {
        System.out.println("Pipeline has been submitted to cluster.");
        System.out.printf("Job ID: %s\n", info.getId());
        System.out.printf("Job Description: %s\n", info.getDescription());
    }
}
