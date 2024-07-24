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
import org.jasypt.encryption.StringEncryptor;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * String Encryptor that delegates always to a {@link org.jasypt.encryption.ByteEncryptor} and
 * converts results to/from Base64 for string representation.
 *
 * <p>Ref: com.ulisesbocchio.jasyptspringboot.encryptor.ByteEncryptorStringEncryptorDelegate
 *
 * @version $Id: $Id
 */
public class ByteEncryptorStringEncryptorDelegate implements StringEncryptor {
    private final ByteEncryptor delegate;

    /**
     * Constructor for ByteEncryptorStringEncryptorDelegate.
     *
     * @param delegate a {@link org.jasypt.encryption.ByteEncryptor} object
     */
    public ByteEncryptorStringEncryptorDelegate(ByteEncryptor delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override
    public String encrypt(String message) {
        return Base64.getEncoder()
                .encodeToString(delegate.encrypt(message.getBytes(StandardCharsets.UTF_8)));
    }

    /** {@inheritDoc} */
    @Override
    public String decrypt(String encryptedMessage) {
        return new String(
                delegate.decrypt(Base64.getDecoder().decode(encryptedMessage)),
                StandardCharsets.UTF_8);
    }
}
