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

import com.ververica.cdc.cli.encryptor.jasypt.AsymmetricCryptography.KeyFormat;
import com.ververica.cdc.cli.encryptor.resource.ByteArrayResource;
import com.ververica.cdc.cli.encryptor.resource.ClassPathResource;
import com.ververica.cdc.cli.encryptor.resource.Resource;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

/**
 * High level configuration class that provides a fallback mechanism to load private/public keys
 * from three different sources in the following order.
 *
 * <p>A Spring Resource
 *
 * <p>A String containing the public/private key
 *
 * <p>A String containing the resource location that contains the public/private key
 *
 * <p>Ref: com.ulisesbocchio.jasyptspringboot.encryptor.SimpleAsymmetricConfig
 *
 * @version $Id: $Id
 */
public class SimpleAsymmetricConfig {

    private String privateKey = null;
    private String publicKey = null;
    private String privateKeyLocation = null;
    private String publicKeyLocation = null;
    private Resource privateKeyResource = null;
    private Resource publicKeyResource = null;
    private KeyFormat privateKeyFormat = KeyFormat.DER;
    private KeyFormat publicKeyFormat = KeyFormat.DER;

    private Resource loadResource(
            Resource asResource,
            String asString,
            String asLocation,
            KeyFormat format,
            String type) {
        return Optional.ofNullable(asResource)
                .orElseGet(
                        () ->
                                Optional.ofNullable(asString)
                                        .map(
                                                pk ->
                                                        (Resource)
                                                                new ByteArrayResource(
                                                                        format == KeyFormat.DER
                                                                                ? Base64
                                                                                        .getDecoder()
                                                                                        .decode(pk)
                                                                                : pk.getBytes(
                                                                                        StandardCharsets
                                                                                                .UTF_8)))
                                        .orElseGet(
                                                () ->
                                                        Optional.ofNullable(asLocation)
                                                                .map(
                                                                        loc ->
                                                                                (Resource)
                                                                                        new ClassPathResource(
                                                                                                loc))
                                                                .orElseThrow(
                                                                        () ->
                                                                                new IllegalArgumentException(
                                                                                        "Unable to load "
                                                                                                + type
                                                                                                + " key. Either resource, key as string, or resource location must be provided"))));
    }

    /**
     * loadPrivateKeyResource.
     *
     * @return a {@link Resource} object
     */
    public Resource loadPrivateKeyResource() {
        return loadResource(
                privateKeyResource, privateKey, privateKeyLocation, privateKeyFormat, "Private");
    }

    /**
     * loadPublicKeyResource.
     *
     * @return a {@link Resource} object
     */
    public Resource loadPublicKeyResource() {
        return loadResource(
                publicKeyResource, publicKey, publicKeyLocation, publicKeyFormat, "Public");
    }

    /**
     * setKeyFormat.
     *
     * @param keyFormat a {@link AsymmetricCryptography.KeyFormat} object
     */
    public void setKeyFormat(KeyFormat keyFormat) {
        setPublicKeyFormat(keyFormat);
        setPrivateKeyFormat(keyFormat);
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public void setPrivateKey(String privateKey) {
        this.privateKey = privateKey;
    }

    public String getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(String publicKey) {
        this.publicKey = publicKey;
    }

    public String getPrivateKeyLocation() {
        return privateKeyLocation;
    }

    public void setPrivateKeyLocation(String privateKeyLocation) {
        this.privateKeyLocation = privateKeyLocation;
    }

    public String getPublicKeyLocation() {
        return publicKeyLocation;
    }

    public void setPublicKeyLocation(String publicKeyLocation) {
        this.publicKeyLocation = publicKeyLocation;
    }

    public Resource getPrivateKeyResource() {
        return privateKeyResource;
    }

    public void setPrivateKeyResource(Resource privateKeyResource) {
        this.privateKeyResource = privateKeyResource;
    }

    public Resource getPublicKeyResource() {
        return publicKeyResource;
    }

    public void setPublicKeyResource(Resource publicKeyResource) {
        this.publicKeyResource = publicKeyResource;
    }

    public KeyFormat getPrivateKeyFormat() {
        return privateKeyFormat;
    }

    public void setPrivateKeyFormat(KeyFormat privateKeyFormat) {
        this.privateKeyFormat = privateKeyFormat;
    }

    public KeyFormat getPublicKeyFormat() {
        return publicKeyFormat;
    }

    public void setPublicKeyFormat(KeyFormat publicKeyFormat) {
        this.publicKeyFormat = publicKeyFormat;
    }
}
