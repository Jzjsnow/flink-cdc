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

package com.ververica.cdc.cli.parser;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import com.ververica.cdc.cli.encryptor.EncryptorPropertyResolver;
import com.ververica.cdc.cli.encryptor.resource.ClassPathResource;
import com.ververica.cdc.cli.encryptor.resource.FileSystemResource;
import com.ververica.cdc.cli.encryptor.resource.Resource;
import com.ververica.cdc.cli.utils.FlinkEnvironmentUtils;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import com.ververica.cdc.composer.definition.PipelineDef;
import com.ververica.cdc.composer.definition.RouteDef;
import com.ververica.cdc.composer.definition.SinkDef;
import com.ververica.cdc.composer.definition.SourceDef;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.ververica.cdc.common.utils.Preconditions.checkNotNull;

/** Parser for converting YAML formatted pipeline definition to {@link PipelineDef}. */
public class YamlPipelineDefinitionParser implements PipelineDefinitionParser {
    private static final Logger LOG = LoggerFactory.getLogger(YamlPipelineDefinitionParser.class);
    // Parent node keys
    private static final String SOURCE_KEY = "source";
    private static final String SINK_KEY = "sink";
    private static final String ROUTE_KEY = "route";
    private static final String PIPELINE_KEY = "pipeline";

    // Source / sink keys
    private static final String TYPE_KEY = "type";
    private static final String NAME_KEY = "name";
    private static final String HOST_LIST = "host_list";
    private static final String COMMA = ",";
    private static final String HOST_NAME = "hostname";
    private static final String PORT = "port";
    private static final String COLON = ":";
    private static final String UDAL = "_udal";

    // Route keys
    private static final String ROUTE_SOURCE_TABLE_KEY = "source-table";
    private static final String ROUTE_SINK_TABLE_KEY = "sink-table";
    private static final String ROUTE_DESCRIPTION_KEY = "description";
    private static final String ICEBERG = "iceberg";

    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    /** Parse the specified pipeline definition file. */
    @Override
    public PipelineDef parse(Path pipelineDefPath, Configuration globalPipelineConfig)
            throws Exception {
        JsonNode root = mapper.readTree(pipelineDefPath.toFile());

        // Pipeline configs are optional
        Configuration userPipelineConfig = toPipelineConfig(root.get(PIPELINE_KEY));

        // Merge user config into global config
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.addAll(globalPipelineConfig);
        pipelineConfig.addAll(userPipelineConfig);

        // Check if any password is encrypted
        boolean isEncrypted = pipelineConfig.get(PipelineOptions.ENCRYPTOR_ENABLED);
        LOG.info("Pipeline definition file encrypted: " + isEncrypted);
        List<SourceDef> sourceDef;
        SinkDef sinkDef;
        if (isEncrypted) {
            if (pipelineConfig.contains(PipelineOptions.ENCRYPTOR_PRIVATE_KEY_LOCATION)
                    && !pipelineConfig.contains(PipelineOptions.ENCRYPTOR_PRIVATE_KEY_STRING)) {
                String privateKeyLocation =
                        pipelineConfig.get(PipelineOptions.ENCRYPTOR_PRIVATE_KEY_LOCATION);

                if (FlinkEnvironmentUtils.checkIfRunningOnApplicationMaster()) {
                    // private key file is already shipped to cluster and in class resources.
                    LOG.info("Using private key in application mode");
                } else {
                    FileSystemResource fileResource = new FileSystemResource(privateKeyLocation);
                    // Check if the private key exists in the filesystem, and add it to the
                    // classpath to
                    // ensure that it will be loaded (Adding private key to the classpath is also to
                    // adapt it to read the key in application mode.)
                    if (!fileResource.exists()) {
                        LOG.error("Private key not found: " + privateKeyLocation);
                        throw new IllegalStateException(
                                "Private key not found: " + privateKeyLocation);
                    }
                    // add it to the classpath
                    addFileToClassPathResources(fileResource.getFile());
                }
            }
            // if encryptor is enabled, decrypt and set the password for the source and sink
            EncryptorPropertyResolver encryptor = new EncryptorPropertyResolver(pipelineConfig);

            // Source is required
            sourceDef = getSourceDefs(root, true, encryptor);

            // Sink is required
            sinkDef =
                    toSinkDef(
                            checkNotNull(
                                    root.get(SINK_KEY),
                                    "Missing required field \"%s\" in pipeline definition",
                                    SINK_KEY),
                            encryptor);
        } else {

            // Source is required
            sourceDef = getSourceDefs(root, false, null);

            // Sink is required
            sinkDef =
                    toSinkDef(
                            checkNotNull(
                                    root.get(SINK_KEY),
                                    "Missing required field \"%s\" in pipeline definition",
                                    SINK_KEY));
        }

        // Routes are optional
        List<RouteDef> routeDefs = new ArrayList<>();
        Optional.ofNullable(root.get(ROUTE_KEY))
                .ifPresent(node -> node.forEach(route -> routeDefs.add(toRouteDef(route))));

        return new PipelineDef(sourceDef, sinkDef, routeDefs, null, pipelineConfig, isEncrypted);
    }

    private List<SourceDef> getSourceDefs(
            JsonNode root, boolean isEncrypted, EncryptorPropertyResolver encryptor) {
        JsonNode sourceNode = root.get(SOURCE_KEY);
        JsonNode hostList = sourceNode.get(HOST_LIST);
        String type = sourceNode.get(TYPE_KEY).asText();
        List<SourceDef> sourceDefs = new ArrayList<>();
        if (hostList != null && type.contains(UDAL)) {
            String hostString = hostList.asText();
            String[] hosts = hostString.split(COMMA);
            Arrays.stream(hosts)
                    .forEach(
                            e -> {
                                ((ObjectNode) sourceNode)
                                        .put(TYPE_KEY, type.substring(0, type.indexOf("_")));
                                ((ObjectNode) sourceNode).put(HOST_NAME, e.split(COLON)[0]);
                                ((ObjectNode) sourceNode).put(PORT, e.split(COLON)[1]);
                                ((ObjectNode) sourceNode).remove(HOST_LIST);
                                getSourceDef(sourceNode, sourceDefs, isEncrypted, encryptor);
                            });
        } else {
            // Source is required
            getSourceDef(sourceNode, sourceDefs, isEncrypted, encryptor);
        }
        return sourceDefs;
    }

    /**
     * Extracts source definitions from a source JSON node and adds them to a list. Depending on
     * whether the source is encrypted, it processes the source node accordingly.
     *
     * @param sourceNode The JSON node containing the source information.
     * @param sourceDefs A list to store the extracted source definitions.
     * @param isEncrypted Indicates whether the source is encrypted. If `isEncrypted` is `false`,
     *     `encryptor` should be `null`.
     * @param encryptor An optional instance of {@link EncryptorPropertyResolver} for decryption,
     *     can be null if the password is not encrypted.
     */
    private void getSourceDef(
            JsonNode sourceNode,
            List<SourceDef> sourceDefs,
            boolean isEncrypted,
            @Nullable EncryptorPropertyResolver encryptor) {
        SourceDef sourceDef;
        if (!isEncrypted) {
            sourceDef =
                    toSourceDef(
                            checkNotNull(
                                    sourceNode,
                                    "Missing required field \"%s\" in pipeline definition",
                                    SOURCE_KEY));
        } else {
            sourceDef =
                    toSourceDef(
                            checkNotNull(
                                    sourceNode,
                                    "Missing required field \"%s\" in pipeline definition",
                                    SOURCE_KEY),
                            encryptor);
        }
        sourceDefs.add(sourceDef);
    }

    /**
     * Converts a source JSON object into a SourceDef object. Note: The source does not contain
     * encrypted passwords, or else use the method {@link #toSourceDef(JsonNode,
     * EncryptorPropertyResolver)}
     *
     * @param sourceNode A JSON object representing the source configuration.
     * @return A SourceDef object converted from the sourceNode.
     */
    private SourceDef toSourceDef(JsonNode sourceNode) {
        return toSourceDef(sourceNode, null);
    }

    /**
     * Converts a source JSON object into a SourceDef object. If an {@link
     * EncryptorPropertyResolver} is provided ({@code encryptor} != null), it will be used to
     * decrypt the password contained in the source configuration.
     *
     * @param sourceNode A JSON object representing the source configuration.
     * @param encryptor An optional instance of {@link EncryptorPropertyResolver} for decryption,
     *     can be null if the password is not encrypted.
     * @return A SourceDef object converted from the sourceNode.
     */
    private SourceDef toSourceDef(
            JsonNode sourceNode, @Nullable EncryptorPropertyResolver encryptor) {
        Map<String, String> sourceMap =
                mapper.convertValue(sourceNode, new TypeReference<Map<String, String>>() {});

        // if an encryptor is provided, attempt to decrypt the password
        if (Objects.nonNull(encryptor)) {
            // check if the password field exists in the source map
            if (sourceMap.containsKey(SourceDef.PASSWORD)) {
                // decrypt the password and update the sourceMap
                String decryptedPassword =
                        encryptor.resolvePropertyValue(sourceMap.get(SourceDef.PASSWORD));
                sourceMap.put(SourceDef.PASSWORD, decryptedPassword);
                LOG.info("Password for source has been decrypted and refreshed");
            } else {
                LOG.info("No password available for source and decryption is skipped");
            }
        }

        // "type" field is required
        String type =
                checkNotNull(
                        sourceMap.remove(TYPE_KEY),
                        "Missing required field \"%s\" in source configuration",
                        TYPE_KEY);

        // "name" field is optional
        String name = sourceMap.remove(NAME_KEY);

        return new SourceDef(type, name, Configuration.fromMap(sourceMap));
    }

    /**
     * Converts a sink JSON node into a SinkDef object. Note: The sink does not contain encrypted
     * passwords, or else use the method {@link #toSinkDef(JsonNode, EncryptorPropertyResolver)}
     *
     * @param sinkNode A JSON node representing the sink configuration.
     * @return A SinkDef object converted from the sinkNode.
     */
    private SinkDef toSinkDef(JsonNode sinkNode) {
        return toSinkDef(sinkNode, null);
    }

    /**
     * Converts a sink JSON node into a SinkDef object. If an {@link EncryptorPropertyResolver} is
     * provided ({@code encryptor} != null), it will be used to decrypt the password contained in
     * the sink configuration.
     *
     * @param sinkNode JSON node representing the sink configuration.
     * @param encryptor An object for resolving encrypted properties, which may be null if the
     *     password is not encrypted.
     * @return A SinkDef object converted from the sinkNode.
     */
    private SinkDef toSinkDef(JsonNode sinkNode, @Nullable EncryptorPropertyResolver encryptor) {
        Map<String, String> sinkMap =
                mapper.convertValue(sinkNode, new TypeReference<Map<String, String>>() {});

        // if an encryptor is provided, attempt to decrypt the password
        if (Objects.nonNull(encryptor)) {
            // check if the password field exists in the sink map
            if (sinkMap.containsKey(SinkDef.PASSWORD)) {
                // decrypt the password and update the sinkMap
                String decryptedPassword =
                        encryptor.resolvePropertyValue(sinkMap.get(SinkDef.PASSWORD));
                sinkMap.put(SinkDef.PASSWORD, decryptedPassword);
                LOG.info("Password for sink has been decrypted and refreshed");
            } else {
                LOG.info("No password available for sink and decryption is skipped");
            }
        }

        // "type" field is required
        String type =
                checkNotNull(
                        sinkMap.remove(TYPE_KEY),
                        "Missing required field \"%s\" in sink configuration",
                        TYPE_KEY);

        if (ICEBERG.equals(type)) {
            // set additional hive conf file content
            setAdditionalHiveConfFileContent(sinkMap);
        }

        // "name" field is optional
        String name = sinkMap.remove(NAME_KEY);

        return new SinkDef(type, name, Configuration.fromMap(sinkMap));
    }

    /**
     * Set additional configuration file contents of Hive. This method adds the content of the
     * hive-site.xml to the sinkMap, which is used in the pipeline configuration.
     *
     * @param sinkMap
     */
    private void setAdditionalHiveConfFileContent(Map<String, String> sinkMap) {
        if (sinkMap.containsKey(SinkDef.HIVE_CONF_LOCATION.key())) {
            String hiveConfLocation = sinkMap.get(SinkDef.HIVE_CONF_LOCATION.key());
            if (hiveConfLocation != null) {
                File hiveConfFile = new File(sinkMap.get(SinkDef.HIVE_CONF_LOCATION.key()));
                if (!hiveConfFile.exists()) {
                    LOG.warn("Hive configuration file {} does not exist", hiveConfFile);
                    return;
                }
                LOG.info("Add Hive configuration file:\n {}", hiveConfFile);
                String xmlContent = null;
                try {
                    xmlContent = FileUtils.readFileToString(hiveConfFile, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                sinkMap.put(SinkDef.HIVE_CONF_FILE_CONTENTS, xmlContent);
            }
        } else {
            LOG.warn("Configuration {} is not set for sink", SinkDef.HIVE_CONF_LOCATION.key());
        }
    }

    private RouteDef toRouteDef(JsonNode routeNode) {
        String sourceTable =
                checkNotNull(
                                routeNode.get(ROUTE_SOURCE_TABLE_KEY),
                                "Missing required field \"%s\" in route configuration",
                                ROUTE_SOURCE_TABLE_KEY)
                        .asText();
        String sinkTable =
                checkNotNull(
                                routeNode.get(ROUTE_SINK_TABLE_KEY),
                                "Missing required field \"%s\" in route configuration",
                                ROUTE_SINK_TABLE_KEY)
                        .asText();
        String description =
                Optional.ofNullable(routeNode.get(ROUTE_DESCRIPTION_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        return new RouteDef(sourceTable, sinkTable, description);
    }

    private Configuration toPipelineConfig(JsonNode pipelineConfigNode) {
        if (pipelineConfigNode == null || pipelineConfigNode.isNull()) {
            return new Configuration();
        }
        Map<String, String> pipelineConfigMap =
                mapper.convertValue(
                        pipelineConfigNode, new TypeReference<Map<String, String>>() {});
        return Configuration.fromMap(pipelineConfigMap);
    }

    /**
     * Check if the file is included in the classpath.
     *
     * @param filename
     */
    private boolean checkFileExistsInClassPathResources(String filename) {
        Resource resource = new ClassPathResource(filename);
        boolean isExists = resource.exists();
        LOG.info("File {} {} in the classpath", filename, isExists ? "exists" : "does not exist");
        return isExists;
    }

    /**
     * add a file to the classpath by adding the parent directory of the file to the classpath.
     *
     * @param file
     * @throws MalformedURLException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    private void addFileToClassPathResources(File file)
            throws MalformedURLException, NoSuchMethodException, InvocationTargetException,
                    IllegalAccessException {
        URL url = file.getParentFile().toURI().toURL();
        Method addURLMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        addURLMethod.setAccessible(true);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        addURLMethod.invoke(classLoader, url);
        LOG.info(url + " added to class path");
    }
}
