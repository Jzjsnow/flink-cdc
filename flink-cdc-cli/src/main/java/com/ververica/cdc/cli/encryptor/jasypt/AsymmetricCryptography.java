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

import com.ververica.cdc.cli.encryptor.resource.ClassPathResource;
import com.ververica.cdc.cli.encryptor.resource.Resource;
import com.ververica.cdc.cli.encryptor.util.FileCopyUtils;

import javax.crypto.Cipher;

import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

/**
 * AsymmetricCryptography class.
 *
 * <p>Ref: com.ulisesbocchio.jasyptspringboot.util.AsymmetricCryptography.
 */
public class AsymmetricCryptography {

    private static final String PRIVATE_KEY_HEADER = "-----BEGIN PRIVATE KEY-----";
    private static final String PUBLIC_KEY_HEADER = "-----BEGIN PUBLIC KEY-----";
    private static final String PRIVATE_KEY_FOOTER = "-----END PRIVATE KEY-----";
    private static final String PUBLIC_KEY_FOOTER = "-----END PUBLIC KEY-----";

    private byte[] getResourceBytes(Resource resource) throws Exception {
        return FileCopyUtils.copyToByteArray(resource.getInputStream());
    }

    private byte[] decodePem(byte[] bytes, String... headers) {
        String pem = new String(bytes, StandardCharsets.UTF_8);
        for (String header : headers) {
            pem = pem.replace(header, "");
        }
        return Base64.getMimeDecoder().decode(pem);
    }

    /**
     * getPrivateKey.
     *
     * @param resourceLocation a {@link java.lang.String} object
     * @param format a {@link AsymmetricCryptography.KeyFormat} object
     * @return a {@link java.security.PrivateKey} object
     */
    public PrivateKey getPrivateKey(String resourceLocation, KeyFormat format) {
        try {
            return getPrivateKey(new ClassPathResource(resourceLocation), format);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * getPrivateKey.
     *
     * @param resource a {@link com.ververica.cdc.cli.encryptor.resource.Resource} object
     * @param format a {@link AsymmetricCryptography.KeyFormat} object
     * @return a {@link java.security.PrivateKey} object
     */
    public PrivateKey getPrivateKey(Resource resource, KeyFormat format) {
        try {
            byte[] keyBytes = getResourceBytes(resource);
            if (format == KeyFormat.PEM) {
                keyBytes = decodePem(keyBytes, PRIVATE_KEY_HEADER, PRIVATE_KEY_FOOTER);
            }
            PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return kf.generatePrivate(spec);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * getPublicKey.
     *
     * @param resourceLocation a {@link java.lang.String} object
     * @param format a {@link AsymmetricCryptography.KeyFormat} object
     * @return a {@link java.security.PublicKey} object
     */
    public PublicKey getPublicKey(String resourceLocation, KeyFormat format) {
        try {
            return getPublicKey(new ClassPathResource(resourceLocation), format);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * getPublicKey.
     *
     * @param resource a {@link com.ververica.cdc.cli.encryptor.resource.Resource} object
     * @param format a {@link AsymmetricCryptography.KeyFormat} object
     * @return a {@link java.security.PublicKey} object
     */
    public PublicKey getPublicKey(Resource resource, KeyFormat format) {
        try {
            byte[] keyBytes = getResourceBytes(resource);
            if (format == KeyFormat.PEM) {
                keyBytes = decodePem(keyBytes, PUBLIC_KEY_HEADER, PUBLIC_KEY_FOOTER);
            }
            X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return kf.generatePublic(spec);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * encrypt.
     *
     * @param msg an array of {@link byte} objects
     * @param key a {@link java.security.PublicKey} object
     * @return an array of {@link byte} objects
     */
    public byte[] encrypt(byte[] msg, PublicKey key) {
        try {
            final Cipher cipher = Cipher.getInstance("RSA");
            cipher.init(Cipher.ENCRYPT_MODE, key);
            return cipher.doFinal(msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * decrypt.
     *
     * @param msg an array of {@link byte} objects
     * @param key a {@link java.security.PrivateKey} object
     * @return an array of {@link byte} objects
     */
    public byte[] decrypt(byte[] msg, PrivateKey key) {
        try {
            final Cipher cipher = Cipher.getInstance("RSA");
            cipher.init(Cipher.DECRYPT_MODE, key);
            return cipher.doFinal(msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Key format. */
    public enum KeyFormat {
        /** DER format. */
        DER,
        /** PEM format. */
        PEM;
    }
}
