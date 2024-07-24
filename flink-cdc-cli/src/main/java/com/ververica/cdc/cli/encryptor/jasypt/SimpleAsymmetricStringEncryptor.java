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

/**
 * {@link org.jasypt.encryption.StringEncryptor} version of {@link SimpleAsymmetricByteEncryptor}
 * that just relies on delegation from {@link ByteEncryptorStringEncryptorDelegate} and provides a
 * constructor for {@link SimpleAsymmetricConfig}.
 *
 * <p>Ref: com.ulisesbocchio.jasyptspringboot.encryptor.SimpleAsymmetricStringEncryptor
 *
 * @version $Id: $Id
 */
public class SimpleAsymmetricStringEncryptor extends ByteEncryptorStringEncryptorDelegate {

    /**
     * Constructor for SimpleAsymmetricStringEncryptor.
     *
     * @param delegate a {@link SimpleAsymmetricByteEncryptor} object
     */
    public SimpleAsymmetricStringEncryptor(SimpleAsymmetricByteEncryptor delegate) {
        super(delegate);
    }

    /**
     * Constructor for SimpleAsymmetricStringEncryptor.
     *
     * @param config a {@link SimpleAsymmetricConfig} object
     */
    public SimpleAsymmetricStringEncryptor(SimpleAsymmetricConfig config) {
        super(new SimpleAsymmetricByteEncryptor(config));
    }
}
