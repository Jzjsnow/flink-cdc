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

import com.ververica.cdc.cli.encryptor.util.ClassUtils;
import com.ververica.cdc.cli.encryptor.util.StringUtils;
import com.ververica.cdc.common.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * {@link Resource} implementation for class path resources. Uses either a given {@link ClassLoader}
 * or a given {@link Class} for loading resources.
 *
 * <p>Ref: org.springframework.core.io.ClassPathResource
 */
public class ClassPathResource implements Resource {

    private final String path;

    @Nullable private ClassLoader classLoader;

    @Nullable private Class<?> clazz;

    /**
     * Create a new {@code ClassPathResource} for {@code ClassLoader} usage. A leading slash will be
     * removed, as the ClassLoader resource access methods will not accept it.
     *
     * <p>The thread context class loader will be used for loading the resource.
     *
     * @param path the absolute path within the class path
     * @see java.lang.ClassLoader#getResourceAsStream(String)
     * @see ClassUtils#getDefaultClassLoader()
     */
    public ClassPathResource(String path) {
        this(path, (ClassLoader) null);
    }

    /**
     * Create a new {@code ClassPathResource} for {@code ClassLoader} usage. A leading slash will be
     * removed, as the ClassLoader resource access methods will not accept it.
     *
     * @param path the absolute path within the classpath
     * @param classLoader the class loader to load the resource with, or {@code null} for the thread
     *     context class loader
     * @see ClassLoader#getResourceAsStream(String)
     */
    public ClassPathResource(String path, @Nullable ClassLoader classLoader) {
        Preconditions.checkNotNull(path, "Path must not be null");
        String pathToUse = StringUtils.cleanPath(path);
        if (pathToUse.startsWith("/")) {
            pathToUse = pathToUse.substring(1);
        }
        this.path = pathToUse;
        this.classLoader = (classLoader != null ? classLoader : ClassUtils.getDefaultClassLoader());
    }

    /**
     * Create a new {@code ClassPathResource} for {@code Class} usage. The path can be relative to
     * the given class, or absolute within the classpath via a leading slash.
     *
     * @param path relative or absolute path within the class path
     * @param clazz the class to load resources with
     * @see java.lang.Class#getResourceAsStream
     */
    public ClassPathResource(String path, @Nullable Class<?> clazz) {
        Preconditions.checkNotNull(path, "Path must not be null");
        this.path = StringUtils.cleanPath(path);
        this.clazz = clazz;
    }

    /** Return the path for this resource (as resource path within the class path). */
    public final String getPath() {
        return this.path;
    }

    /** Return the {@link ClassLoader} that this resource will be obtained from. */
    @Nullable
    public final ClassLoader getClassLoader() {
        return (this.clazz != null ? this.clazz.getClassLoader() : this.classLoader);
    }

    /**
     * This implementation checks for the resolution of a resource URL.
     *
     * @see ClassLoader#getResource(String)
     * @see Class#getResource(String)
     */
    @Override
    public boolean exists() {
        return (resolveURL() != null);
    }

    /**
     * Resolves a URL for the underlying class path resource.
     *
     * @return the resolved URL, or {@code null} if not resolvable
     */
    @Nullable
    protected URL resolveURL() {
        try {
            if (this.clazz != null) {
                return this.clazz.getResource(this.path);
            } else if (this.classLoader != null) {
                return this.classLoader.getResource(this.path);
            } else {
                return ClassLoader.getSystemResource(this.path);
            }
        } catch (IllegalArgumentException ex) {
            // Should not happen according to the JDK's contract:
            // see https://github.com/openjdk/jdk/pull/2662
            return null;
        }
    }

    /**
     * This implementation opens an {@link InputStream} for the underlying class path resource, if
     * available.
     *
     * @see ClassLoader#getResourceAsStream(String)
     * @see Class#getResourceAsStream(String)
     * @see ClassLoader#getSystemResourceAsStream(String)
     */
    @Override
    public InputStream getInputStream() throws IOException {
        return getInputStream(true);
    }

    public InputStream getInputStream(boolean useLatestResource) throws IOException {
        InputStream is;
        if (this.clazz != null) {
            is = this.clazz.getResourceAsStream(this.path);
        } else if (this.classLoader != null) {
            // if using the latest resource file, select the last url in the resource list
            if (useLatestResource) {
                Enumeration<URL> urls = this.classLoader.getResources(this.path);
                List<URL> urlsList = Collections.list(urls);
                // the last url is the latest file
                URL latestURL = urlsList.get(urlsList.size() - 1);
                is = latestURL != null ? latestURL.openStream() : null;
            } else {
                // getResourceAsStream method uses the first url in the resource list by default
                is = this.classLoader.getResourceAsStream(this.path);
            }
        } else {
            is = ClassLoader.getSystemResourceAsStream(this.path);
        }
        if (is == null) {
            throw new FileNotFoundException(
                    getDescription() + " cannot be opened because it does not exist");
        }
        return is;
    }

    /**
     * This implementation returns a URL for the underlying class path resource, if available.
     *
     * @see ClassLoader#getResource(String)
     * @see Class#getResource(String)
     */
    @Override
    public URL getURL() throws IOException {
        URL url = resolveURL();
        if (url == null) {
            throw new FileNotFoundException(
                    getDescription() + " cannot be resolved to URL because it does not exist");
        }
        return url;
    }

    /**
     * This implementation returns the name of the file that this class path resource refers to.
     *
     * @see StringUtils#getFilename(String)
     */
    @Override
    @Nullable
    public String getFilename() {
        return StringUtils.getFilename(this.path);
    }

    /** This implementation returns a description that includes the class path location. */
    @Override
    public String getDescription() {
        StringBuilder builder = new StringBuilder("class path resource [");
        String pathToUse = this.path;
        if (this.clazz != null && !pathToUse.startsWith("/")) {
            builder.append(ClassUtils.classPackageAsResourcePath(this.clazz));
            builder.append('/');
        }
        if (pathToUse.startsWith("/")) {
            pathToUse = pathToUse.substring(1);
        }
        builder.append(pathToUse);
        builder.append(']');
        return builder.toString();
    }

    /** This implementation compares the underlying class path locations. */
    @Override
    public boolean equals(@Nullable Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ClassPathResource)) {
            return false;
        }
        ClassPathResource otherRes = (ClassPathResource) other;
        return (this.path.equals(otherRes.path)
                && nullSafeEquals(this.classLoader, otherRes.classLoader)
                && nullSafeEquals(this.clazz, otherRes.clazz));
    }

    /** This implementation returns the hash code of the underlying class path location. */
    @Override
    public int hashCode() {
        return this.path.hashCode();
    }

    private boolean nullSafeEquals(@Nullable Object o1, @Nullable Object o2) {
        if (o1 == o2) {
            return true;
        }
        if (o1 == null || o2 == null) {
            return false;
        }
        if (o1.equals(o2)) {
            return true;
        }
        if (o1.getClass().isArray() && o2.getClass().isArray()) {
            return arrayEquals(o1, o2);
        }
        return false;
    }

    /**
     * Compare the given arrays with {@code Arrays.equals}, performing an equality check based on
     * the array elements rather than the array reference.
     *
     * @param o1 first array to compare
     * @param o2 second array to compare
     * @return whether the given objects are equal
     * @see #nullSafeEquals(Object, Object)
     * @see java.util.Arrays#equals
     */
    private boolean arrayEquals(Object o1, Object o2) {
        if (o1 instanceof Object[] && o2 instanceof Object[]) {
            return Arrays.equals((Object[]) o1, (Object[]) o2);
        }
        if (o1 instanceof boolean[] && o2 instanceof boolean[]) {
            return Arrays.equals((boolean[]) o1, (boolean[]) o2);
        }
        if (o1 instanceof byte[] && o2 instanceof byte[]) {
            return Arrays.equals((byte[]) o1, (byte[]) o2);
        }
        if (o1 instanceof char[] && o2 instanceof char[]) {
            return Arrays.equals((char[]) o1, (char[]) o2);
        }
        if (o1 instanceof double[] && o2 instanceof double[]) {
            return Arrays.equals((double[]) o1, (double[]) o2);
        }
        if (o1 instanceof float[] && o2 instanceof float[]) {
            return Arrays.equals((float[]) o1, (float[]) o2);
        }
        if (o1 instanceof int[] && o2 instanceof int[]) {
            return Arrays.equals((int[]) o1, (int[]) o2);
        }
        if (o1 instanceof long[] && o2 instanceof long[]) {
            return Arrays.equals((long[]) o1, (long[]) o2);
        }
        if (o1 instanceof short[] && o2 instanceof short[]) {
            return Arrays.equals((short[]) o1, (short[]) o2);
        }
        return false;
    }
}
