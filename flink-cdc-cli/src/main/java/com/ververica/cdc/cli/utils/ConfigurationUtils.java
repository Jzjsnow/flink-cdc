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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import com.ververica.cdc.common.configuration.Configuration;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utilities for handling {@link Configuration}. */
public class ConfigurationUtils {
    private static final String COMMAND_LINE_PROPERTY_PREFIX = "-D";
    private static final String COMMAND_LINE_PROPERTY_IDENTIFIER = "D";
    private static final String COMMAND_LINE_PROPERTY_DELIMITER = "=";

    public static Configuration loadMapFormattedConfig(Path configPath) throws Exception {
        if (!Files.exists(configPath)) {
            throw new FileNotFoundException(
                    String.format("Cannot find configuration file at \"%s\"", configPath));
        }
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            Map<String, String> configMap =
                    mapper.readValue(
                            configPath.toFile(), new TypeReference<Map<String, String>>() {});
            return Configuration.fromMap(configMap);
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to load config file \"%s\" to key-value pairs", configPath),
                    e);
        }
    }

    /**
     * Convert properties in the command line that begin with “-D” to “D” to ensure that the
     * properties can be parsed into the command line args.
     *
     * @param args
     * @return
     */
    public static String[] filterProperties(String[] args) {
        return Arrays.stream(args)
                .map(
                        arg -> {
                            if (arg.startsWith(COMMAND_LINE_PROPERTY_PREFIX)
                                    && arg.contains(COMMAND_LINE_PROPERTY_DELIMITER)) {
                                return arg.substring(1);
                            } else {
                                return arg;
                            }
                        })
                .toArray(String[]::new);
    }

    public static Configuration getCliConfiguration(List<String> unparsedArgs) {
        List<String> flinkArgs =
                unparsedArgs.stream()
                        .filter(ConfigurationUtils::isValidProperty)
                        .collect(Collectors.toList());
        Map<String, String> flinkProperties = new HashMap<>();
        for (String flinkArg : flinkArgs) {
            List<String> property = parseProperties(flinkArg);
            if (!property.isEmpty()) {
                flinkProperties.put(property.get(0), property.get(1));
            }
        }
        return Configuration.fromMap(flinkProperties);
    }

    private static boolean isValidProperty(String property) {
        return // check if the property starts with D
        property.startsWith(COMMAND_LINE_PROPERTY_IDENTIFIER)
                // check if the property contains both key and value
                && property.contains(COMMAND_LINE_PROPERTY_DELIMITER)
                // Check if the key is not empty
                && property.indexOf(COMMAND_LINE_PROPERTY_DELIMITER) > 1
                // Check if the value is not empty
                && property.length() > property.indexOf(COMMAND_LINE_PROPERTY_DELIMITER) + 1;
    }

    private static List<String> parseProperties(String keyValue) {
        List<String> properties = new ArrayList<>();
        final int pos = keyValue.indexOf('=');

        final String key = keyValue.substring(1, pos);
        final String value = keyValue.substring(pos + 1);
        properties.add(key);
        properties.add(value);

        return properties;
    }
}
