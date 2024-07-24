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

package com.ververica.cdc.cli.encryptor.jasypt;

import org.jasypt.encryption.ByteEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Objects;

/**
 * Vanilla implementation of an asymmetric encryptor that relies on {@link AsymmetricCryptography}
 * Keys are lazily loaded from {@link SimpleAsymmetricConfig}.
 *
 * <p>Ref: com.ulisesbocchio.jasyptspringboot.encryptor.SimpleAsymmetricByteEncryptor
 *
 * @version $Id: $Id
 */
public class SimpleAsymmetricByteEncryptor implements ByteEncryptor {

    public static final Logger LOG = LoggerFactory.getLogger(SimpleAsymmetricByteEncryptor.class);
    private final AsymmetricCryptography crypto;
    private final PublicKey publicKey;
    private final PrivateKey privateKey;

    /**
     * Constructor for SimpleAsymmetricByteEncryptor.
     *
     * @param config a {@link SimpleAsymmetricConfig} object
     */
    public SimpleAsymmetricByteEncryptor(SimpleAsymmetricConfig config) {
        PrivateKey privateKeyFromResource = null;
        PublicKey publicKeyFromResource = null;
        crypto = new AsymmetricCryptography();
        try {
            privateKeyFromResource =
                    crypto.getPrivateKey(
                            config.loadPrivateKeyResource(), config.getPrivateKeyFormat());
        } catch (Exception e) {
            LOG.info("Private key not obtained");
        }
        try {
            publicKeyFromResource =
                    crypto.getPublicKey(
                            config.loadPublicKeyResource(), config.getPublicKeyFormat());
        } catch (Exception e) {
            LOG.info("Public key not obtained");
        }
        if (Objects.isNull(privateKeyFromResource) && Objects.isNull(publicKeyFromResource)) {
            throw new IllegalArgumentException(
                    "Unable to load Public/Private key. Either resource, key as string, or resource location must be provided");
        }
        privateKey = privateKeyFromResource;
        publicKey = publicKeyFromResource;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] encrypt(byte[] message) {
        return this.crypto.encrypt(message, publicKey);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] decrypt(byte[] encryptedMessage) {
        return this.crypto.decrypt(encryptedMessage, privateKey);
    }
}
