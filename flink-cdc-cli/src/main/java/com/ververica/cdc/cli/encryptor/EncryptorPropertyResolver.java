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

import com.ververica.cdc.cli.encryptor.jasypt.AsymmetricCryptography;
import com.ververica.cdc.cli.encryptor.jasypt.EncryptablePropertyResolver;
import com.ververica.cdc.cli.encryptor.jasypt.SimpleAsymmetricConfig;
import com.ververica.cdc.cli.encryptor.jasypt.SimpleAsymmetricStringEncryptor;
import com.ververica.cdc.cli.encryptor.resource.FileSystemResource;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import org.jasypt.encryption.StringEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Encryptor initiated from configurations. */
public class EncryptorPropertyResolver implements EncryptablePropertyResolver {
    public static final Logger LOG = LoggerFactory.getLogger(EncryptorPropertyResolver.class);
    public static final String PRIVATE_KEY_FORMAT_DER = "DER";
    public static final String PRIVATE_KEY_FORMAT_PEM = "PEM";
    private final StringEncryptor encryptor;
    private final Configuration pipelineConfigs;
    private SimpleAsymmetricConfig config;

    public EncryptorPropertyResolver(Configuration pipelineConfigs) {
        this.pipelineConfigs = pipelineConfigs;
        this.config = new SimpleAsymmetricConfig();
        setPrivateKeyFormat(config);
        setPrivateKeyContent(config);

        encryptor = new SimpleAsymmetricStringEncryptor(config);
        LOG.info("String encryptor resolver with private key initialized");
    }

    /**
     * set private key format for decryption.
     *
     * @param config
     * @return
     */
    private void setPrivateKeyFormat(SimpleAsymmetricConfig config) {
        if (!pipelineConfigs.contains(PipelineOptions.ENCRYPTOR_PRIVATE_KEY_FORMAT)) {
            LOG.info(
                    "Private key format is unset, use default value {}",
                    config.getPrivateKeyFormat());
        }
        if (PRIVATE_KEY_FORMAT_DER.equals(
                pipelineConfigs.get(PipelineOptions.ENCRYPTOR_PRIVATE_KEY_FORMAT))) {
            config.setPrivateKeyFormat(AsymmetricCryptography.KeyFormat.DER);
        } else if (PRIVATE_KEY_FORMAT_PEM.equals(
                pipelineConfigs.get(PipelineOptions.ENCRYPTOR_PRIVATE_KEY_FORMAT))) {
            config.setPrivateKeyFormat(AsymmetricCryptography.KeyFormat.PEM);
        } else {
            LOG.error("Invalid private key format specified");
            throw new IllegalArgumentException(
                    "Invalid private key format specified, only DER or PEM is supported");
        }
    }

    private void setPrivateKeyContent(SimpleAsymmetricConfig config) {
        if (pipelineConfigs.contains(PipelineOptions.ENCRYPTOR_PRIVATE_KEY_STRING)) {
            config.setPrivateKey(pipelineConfigs.get(PipelineOptions.ENCRYPTOR_PRIVATE_KEY_STRING));
        } else if (pipelineConfigs.contains(PipelineOptions.ENCRYPTOR_PRIVATE_KEY_LOCATION)) {
            String keyFile =
                    new FileSystemResource(
                                    pipelineConfigs.get(
                                            PipelineOptions.ENCRYPTOR_PRIVATE_KEY_LOCATION))
                            .getFilename();
            config.setPrivateKeyLocation(keyFile);
        } else {
            LOG.error(
                    "Invalid private key configuration： {}/{} is required",
                    PipelineOptions.ENCRYPTOR_PRIVATE_KEY_STRING.key(),
                    PipelineOptions.ENCRYPTOR_PRIVATE_KEY_LOCATION.key());
            throw new IllegalArgumentException(
                    "Invalid private key configuration: "
                            + PipelineOptions.ENCRYPTOR_PRIVATE_KEY_STRING.key()
                            + "/"
                            + PipelineOptions.ENCRYPTOR_PRIVATE_KEY_LOCATION.key()
                            + " is required");
        }
    }

    @Override
    public String resolvePropertyValue(String value) {
        if (value != null && !value.isEmpty()) {
            return encryptor.decrypt(value);
        }
        return value;
    }
}
