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

import com.ververica.cdc.cli.encryptor.util.StringUtils;
import com.ververica.cdc.common.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

/**
 * {@link Resource} implementation for {@code java.io.File} and {@code java.nio.file.Path} handles
 * with a file system target. Supports resolution as a {@code File} and also as a {@code URL}.
 * Implements the extended interface.
 */
public class FileSystemResource implements Resource {
    private final String path;

    @Nullable private final File file;

    private final Path filePath;

    /**
     * Create a new {@code FileSystemResource} from a file path.
     *
     * @param path a file path
     * @see #FileSystemResource(Path)
     */
    public FileSystemResource(String path) {
        Preconditions.checkNotNull(path, "Path must not be null");
        this.path = StringUtils.cleanPath(path);
        this.file = new File(path);
        this.filePath = this.file.toPath();
    }

    /**
     * Create a new {@code FileSystemResource} from a {@link Path} handle, performing all file
     * system interactions via NIO.2 instead of {@link File}.
     *
     * @param filePath a Path handle to a file
     * @since 5.1
     */
    public FileSystemResource(Path filePath) {
        Preconditions.checkNotNull(filePath, "Path must not be null");
        this.path = StringUtils.cleanPath(filePath.toString());
        this.file = null;
        this.filePath = filePath;
    }

    /**
     * Create a new {@code FileSystemResource} from a {@link FileSystem} handle, locating the
     * specified path.
     *
     * <p>This is an alternative to {@link #FileSystemResource(String)}, performing all file system
     * interactions via NIO.2 instead of {@link File}.
     *
     * @param fileSystem the FileSystem to locate the path within
     * @param path a file path
     * @since 5.1.1
     */
    public FileSystemResource(FileSystem fileSystem, String path) {
        Preconditions.checkNotNull(fileSystem, "FileSystem must not be null");
        Preconditions.checkNotNull(path, "Path must not be null");
        this.path = StringUtils.cleanPath(path);
        this.file = null;
        this.filePath = fileSystem.getPath(this.path).normalize();
    }

    /** Return the file path for this resource. */
    public final String getPath() {
        return this.path;
    }

    @Override
    public boolean exists() {
        return (this.file != null ? this.file.exists() : Files.exists(this.filePath));
    }

    @Override
    public URL getURL() throws IOException {
        return (this.file != null ? this.file.toURI().toURL() : this.filePath.toUri().toURL());
    }

    @Nullable
    @Override
    public String getFilename() {
        return (this.file != null ? this.file.getName() : this.filePath.getFileName().toString());
    }

    public File getFile() {
        return (this.file != null ? this.file : this.filePath.toFile());
    }

    @Override
    public String getDescription() {
        return "file ["
                + (this.file != null ? this.file.getAbsolutePath() : this.filePath.toAbsolutePath())
                + "]";
    }

    @Override
    public InputStream getInputStream() throws Exception {
        try {
            return Files.newInputStream(this.filePath);
        } catch (NoSuchFileException ex) {
            throw new FileNotFoundException(ex.getMessage());
        }
    }
}
