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

package com.ververica.cdc.cli.encryptor;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.twitter.chill.Base64;
import com.ververica.cdc.cli.encryptor.jasypt.AsymmetricCryptography;
import com.ververica.cdc.cli.encryptor.jasypt.SimpleAsymmetricConfig;
import com.ververica.cdc.cli.encryptor.jasypt.SimpleAsymmetricStringEncryptor;
import com.ververica.cdc.cli.encryptor.resource.FileSystemResource;
import com.ververica.cdc.cli.utils.FlinkEnvironmentUtils;
import com.ververica.cdc.common.annotation.VisibleForTesting;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.ConfigOptions;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.shade.ConfigShade;
import org.jasypt.encryption.StringEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;

/** rsa encryptor ConfigShade. */
public class RSAConfigShade implements ConfigShade {
    public static final Logger LOG = LoggerFactory.getLogger(RSAConfigShade.class);
    private static final String IDENTIFIER = "rsa";
    public static final String PRIVATE_KEY_FORMAT_DER = "DER";
    public static final String PRIVATE_KEY_FORMAT_PEM = "PEM";
    private StringEncryptor encryptor;
    private Configuration pipelineConfigs;

    @VisibleForTesting
    public static final ConfigOption<String> ENCRYPTOR_PRIVATE_KEY_FORMAT =
            ConfigOptions.key("jasypt-encryptor.private-key-format")
                    .stringType()
                    .defaultValue("DER")
                    .withDescription("The format of private key, DER or PEM.");

    @VisibleForTesting
    public static final ConfigOption<String> ENCRYPTOR_PRIVATE_KEY_STRING =
            ConfigOptions.key("jasypt-encryptor.private-key-string")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The private key for decryption in String format.");

    @VisibleForTesting
    public static final ConfigOption<String> ENCRYPTOR_PRIVATE_KEY_LOCATION =
            ConfigOptions.key("jasypt-encryptor.private-key-location")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The location of the private key for decryption.");

    @VisibleForTesting
    public static final org.apache.flink.configuration.ConfigOption<String>
            SECRET_ENCRYPTOR_PRIVATE_KEY_STRING =
                    org.apache.flink.configuration.ConfigOptions.key(
                                    "secret.jasypt-encryptor.private-key-string")
                            .stringType()
                            .noDefaultValue()
                            .withDescription("The private key for decryption in String format.");

    @Override
    public void initialize(Configuration pipelineConfig) throws Exception {
        this.pipelineConfigs = pipelineConfig;
        SimpleAsymmetricConfig config = new SimpleAsymmetricConfig();
        setPrivateKeyFormat(config);
        setPrivateKeyContent(config);

        encryptor = new SimpleAsymmetricStringEncryptor(config);
        LOG.info("String encryptor resolver with private key initialized");
    }

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public String decrypt(String content) {
        if (content != null && !content.isEmpty()) {
            return encryptor.decrypt(content);
        }
        return content;
    }

    /**
     * set the private key format for decryption. This method checks the private key format in the
     * pipelineConfigs and updates the SimpleAsymmetricConfig instance accordingly.
     *
     * @param config the SimpleAsymmetricConfig instance to set the private key format
     * @return
     */
    private void setPrivateKeyFormat(SimpleAsymmetricConfig config) {
        // Check if the private key format is not set, and use the default value if not
        if (!pipelineConfigs.contains(ENCRYPTOR_PRIVATE_KEY_FORMAT)) {
            LOG.info(
                    "Private key format is unset, use default value {}",
                    config.getPrivateKeyFormat());
        }
        // Set the private key format based on the configurations
        if (PRIVATE_KEY_FORMAT_DER.equals(pipelineConfigs.get(ENCRYPTOR_PRIVATE_KEY_FORMAT))) {
            config.setPrivateKeyFormat(AsymmetricCryptography.KeyFormat.DER);
        } else if (PRIVATE_KEY_FORMAT_PEM.equals(
                pipelineConfigs.get(ENCRYPTOR_PRIVATE_KEY_FORMAT))) {
            config.setPrivateKeyFormat(AsymmetricCryptography.KeyFormat.PEM);
        } else {
            LOG.error("Invalid private key format specified");
            throw new IllegalArgumentException(
                    "Invalid private key format specified, only DER or PEM is supported");
        }
    }

    /**
     * Sets the private key content for the asymmetric encryption configuration. This method checks
     * if the `pipelineConfigs` contains either the private key string or the private key file
     * location. If the private key string is present, it sets the private key content directly. If
     * the private key file location is present, it reads the file content and sets to the private
     * key content. If neither is present, it logs an error and throws an
     * `IllegalArgumentException`.
     *
     * @param config The asymmetric encryption configuration object to set the private key content
     *     or location.
     * @throws IllegalArgumentException If neither the private key string nor the private key file
     *     location is present in the configuration.
     */
    private void setPrivateKeyContent(SimpleAsymmetricConfig config) throws Exception {
        if (pipelineConfigs.contains(ENCRYPTOR_PRIVATE_KEY_STRING)) {
            config.setPrivateKey(pipelineConfigs.get(ENCRYPTOR_PRIVATE_KEY_STRING));
        } else if (pipelineConfigs.contains(ENCRYPTOR_PRIVATE_KEY_LOCATION)) {
            FileSystemResource keyFileResource =
                    new FileSystemResource(pipelineConfigs.get(ENCRYPTOR_PRIVATE_KEY_LOCATION));

            if (FlinkEnvironmentUtils.checkIfRunningOnApplicationMaster()) {
                // private key file content is already add in the configuration.
                LOG.info("Get private key in application mode");
                String privateKeyString = getFromFlinkConfigs(SECRET_ENCRYPTOR_PRIVATE_KEY_STRING);
                if (privateKeyString != null) {
                    LOG.info("Set private key string");
                    config.setPrivateKey(privateKeyString);
                } else {
                    LOG.warn("Private key is null");
                }
            } else {
                // Check if the private key exists in the filesystem, and add it to the
                // classpath to ensure that it will be loaded (Adding private key to the classpath
                // is also to
                // adapt it to read the key in application mode.)
                if (!keyFileResource.exists()) {
                    LOG.error("Private key not found: " + keyFileResource.getPath());
                    throw new IllegalStateException(
                            "Private key not found: " + keyFileResource.getPath());
                }
                String privateKeyString;
                switch (pipelineConfigs.get(ENCRYPTOR_PRIVATE_KEY_FORMAT)) {
                    case PRIVATE_KEY_FORMAT_DER:
                        privateKeyString =
                                Base64.encodeBytes(
                                        (Files.readAllBytes(keyFileResource.getFile().toPath())));
                        break;
                    case PRIVATE_KEY_FORMAT_PEM:
                        privateKeyString =
                                new String(Files.readAllBytes(keyFileResource.getFile().toPath()));
                        break;
                    default:
                        LOG.error("Invalid private key format specified");
                        throw new IllegalArgumentException(
                                "Invalid private key format specified, only DER or PEM is supported");
                }

                ConfigShade.addFlinkConfiguration(
                        SECRET_ENCRYPTOR_PRIVATE_KEY_STRING.key(),
                        privateKeyString,
                        pipelineConfigs);

                config.setPrivateKey(privateKeyString);
            }
            config.setPrivateKeyLocation(keyFileResource.getFilename());
        } else {
            LOG.error(
                    "Invalid private key configuration： {}/{} is required",
                    ENCRYPTOR_PRIVATE_KEY_STRING.key(),
                    ENCRYPTOR_PRIVATE_KEY_LOCATION.key());
            throw new IllegalArgumentException(
                    "Invalid private key configuration: "
                            + ENCRYPTOR_PRIVATE_KEY_STRING.key()
                            + "/"
                            + ENCRYPTOR_PRIVATE_KEY_LOCATION.key()
                            + " is required");
        }
    }

    public static <T> T getFromFlinkConfigs(
            org.apache.flink.configuration.ConfigOption<T> configOption) {
        LOG.info("Get {} from configurations of the environment", configOption.key());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        return env.getConfiguration().get(configOption);
    }
}
