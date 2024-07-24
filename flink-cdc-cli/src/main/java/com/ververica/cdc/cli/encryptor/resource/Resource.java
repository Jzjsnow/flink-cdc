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

package com.ververica.cdc.cli.encryptor.resource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Interface for a resource descriptor that abstracts from the actual type of underlying resource,
 * such as a file or class path resource.
 *
 * <p>An InputStream can be opened for every resource if it exists in physical form, but a URL or
 * File handle can just be returned for certain resources. The actual behavior is
 * implementation-specific.
 *
 * <p>Ref: org.springframework.core.io.Resource
 */
public interface Resource {

    /**
     * Determine whether this resource actually exists in physical form.
     *
     * <p>This method performs a definitive existence check, whereas the existence of a {@code
     * Resource} handle only guarantees a valid descriptor handle.
     */
    boolean exists();

    /**
     * Indicate whether this resource represents a handle with an open stream. If {@code true}, the
     * InputStream cannot be read multiple times, and must be read and closed to avoid resource
     * leaks.
     *
     * <p>Will be {@code false} for typical resource descriptors.
     */
    default boolean isOpen() {
        return false;
    }

    /**
     * Determine whether this resource represents a file in a file system.
     *
     * <p>A value of {@code true} strongly suggests (but does not guarantee) that a getFile() call
     * will succeed.
     *
     * <p>This is conservatively {@code false} by default.
     *
     * @since 5.0
     */
    default boolean isFile() {
        return false;
    }

    /**
     * Return a URL handle for this resource.
     *
     * @throws IOException if the resource cannot be resolved as URL, i.e. if the resource is not
     *     available as a descriptor
     */
    URL getURL() throws IOException;

    /**
     * Determine a filename for this resource, i.e. typically the last part of the path: for
     * example, "myfile.txt".
     *
     * <p>Returns {@code null} if this type of resource does not have a filename.
     */
    @Nullable
    String getFilename();

    /**
     * Return a description for this resource, to be used for error output when working with the
     * resource.
     *
     * <p>Implementations are also encouraged to return this value from their {@code toString}
     * method.
     *
     * @see Object#toString()
     */
    String getDescription();

    /**
     * Return an {@link InputStream} for the content of an underlying resource.
     *
     * <p>It is expected that each call creates a <i>fresh</i> stream.
     *
     * <p>This requirement is particularly important when you consider an API such as JavaMail,
     * which needs to be able to read the stream multiple times when creating mail attachments. For
     * such a use case, it is <i>required</i> that each {@code getInputStream()} call returns a
     * fresh stream.
     *
     * @return the input stream for the underlying resource (must not be {@code null})
     * @throws java.io.FileNotFoundException if the underlying resource does not exist
     * @throws IOException if the content stream could not be opened
     */
    InputStream getInputStream() throws Exception;
}
