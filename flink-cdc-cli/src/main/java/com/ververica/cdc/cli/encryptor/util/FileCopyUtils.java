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

package com.ververica.cdc.cli.encryptor.util;

import com.ververica.cdc.common.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;

/**
 * Simple utility methods for file and stream copying. All copy methods use a block size of 4096
 * bytes, and close all affected streams when done.
 *
 * <p>Ref: org.springframework.util.FileCopyUtils
 */
public abstract class FileCopyUtils {

    /** The default buffer size used when copying bytes. */
    public static final int BUFFER_SIZE = 4096;

    // ---------------------------------------------------------------------
    // Copy methods for java.io.File
    // ---------------------------------------------------------------------

    /**
     * Copy the contents of the given input File to the given output File.
     *
     * @param in the file to copy from
     * @param out the file to copy to
     * @return the number of bytes copied
     * @throws IOException in case of I/O errors
     */
    public static int copy(File in, File out) throws IOException {
        Preconditions.checkNotNull(in, "No input File specified");
        Preconditions.checkNotNull(out, "No output File specified");
        return copy(Files.newInputStream(in.toPath()), Files.newOutputStream(out.toPath()));
    }

    /**
     * Copy the contents of the given byte array to the given output File.
     *
     * @param in the byte array to copy from
     * @param out the file to copy to
     * @throws IOException in case of I/O errors
     */
    public static void copy(byte[] in, File out) throws IOException {
        Preconditions.checkNotNull(in, "No input byte array specified");
        Preconditions.checkNotNull(out, "No output File specified");
        copy(new ByteArrayInputStream(in), Files.newOutputStream(out.toPath()));
    }

    /**
     * Copy the contents of the given input File into a new byte array.
     *
     * @param in the file to copy from
     * @return the new byte array that has been copied to
     * @throws IOException in case of I/O errors
     */
    public static byte[] copyToByteArray(File in) throws IOException {
        Preconditions.checkNotNull(in, "No input File specified");
        return copyToByteArray(Files.newInputStream(in.toPath()));
    }

    // ---------------------------------------------------------------------
    // Copy methods for java.io.InputStream / java.io.OutputStream
    // ---------------------------------------------------------------------

    /**
     * Copy the contents of the given InputStream to the given OutputStream. Closes both streams
     * when done.
     *
     * @param in the stream to copy from
     * @param out the stream to copy to
     * @return the number of bytes copied
     * @throws IOException in case of I/O errors
     */
    public static int copy(InputStream in, OutputStream out) throws IOException {
        Preconditions.checkNotNull(in, "No InputStream specified");
        Preconditions.checkNotNull(out, "No OutputStream specified");

        try {
            Preconditions.checkNotNull(in, "No InputStream specified");
            Preconditions.checkNotNull(out, "No OutputStream specified");

            int byteCount = 0;
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                byteCount += bytesRead;
            }
            out.flush();
            return byteCount;
        } finally {
            close(in);
            close(out);
        }
    }

    /**
     * Copy the contents of the given byte array to the given OutputStream. Closes the stream when
     * done.
     *
     * @param in the byte array to copy from
     * @param out the OutputStream to copy to
     * @throws IOException in case of I/O errors
     */
    public static void copy(byte[] in, OutputStream out) throws IOException {
        Preconditions.checkNotNull(in, "No input byte array specified");
        Preconditions.checkNotNull(out, "No OutputStream specified");

        try {
            out.write(in);
        } finally {
            close(out);
        }
    }

    /**
     * Copy the contents of the given InputStream into a new byte array. Closes the stream when
     * done.
     *
     * @param in the stream to copy from (may be {@code null} or empty)
     * @return the new byte array that has been copied to (possibly empty)
     * @throws IOException in case of I/O errors
     */
    public static byte[] copyToByteArray(@Nullable InputStream in) throws IOException {
        if (in == null) {
            return new byte[0];
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream(BUFFER_SIZE);
        copy(in, out);
        return out.toByteArray();
    }

    // ---------------------------------------------------------------------
    // Copy methods for java.io.Reader / java.io.Writer
    // ---------------------------------------------------------------------

    /**
     * Copy the contents of the given Reader to the given Writer. Closes both when done.
     *
     * @param in the Reader to copy from
     * @param out the Writer to copy to
     * @return the number of characters copied
     * @throws IOException in case of I/O errors
     */
    public static int copy(Reader in, Writer out) throws IOException {
        Preconditions.checkNotNull(in, "No Reader specified");
        Preconditions.checkNotNull(out, "No Writer specified");

        try {
            int charCount = 0;
            char[] buffer = new char[BUFFER_SIZE];
            int charsRead;
            while ((charsRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, charsRead);
                charCount += charsRead;
            }
            out.flush();
            return charCount;
        } finally {
            close(in);
            close(out);
        }
    }

    /**
     * Copy the contents of the given String to the given Writer. Closes the writer when done.
     *
     * @param in the String to copy from
     * @param out the Writer to copy to
     * @throws IOException in case of I/O errors
     */
    public static void copy(String in, Writer out) throws IOException {
        Preconditions.checkNotNull(in, "No input String specified");
        Preconditions.checkNotNull(out, "No Writer specified");

        try {
            out.write(in);
        } finally {
            close(out);
        }
    }

    /**
     * Copy the contents of the given Reader into a String. Closes the reader when done.
     *
     * @param in the reader to copy from (may be {@code null} or empty)
     * @return the String that has been copied to (possibly empty)
     * @throws IOException in case of I/O errors
     */
    public static String copyToString(@Nullable Reader in) throws IOException {
        if (in == null) {
            return "";
        }

        StringWriter out = new StringWriter(BUFFER_SIZE);
        copy(in, out);
        return out.toString();
    }

    /**
     * Attempt to close the supplied {@link Closeable}, silently swallowing any exceptions.
     *
     * @param closeable the {@code Closeable} to close
     */
    private static void close(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException ex) {
            // ignore
        }
    }
}
