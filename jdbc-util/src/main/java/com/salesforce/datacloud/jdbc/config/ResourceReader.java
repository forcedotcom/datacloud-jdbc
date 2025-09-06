/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.config;

import com.google.common.io.ByteStreams;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public final class ResourceReader {
    private ResourceReader() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static String readResourceAsString(@NonNull String path) {
        val result = new AtomicReference<String>();
        withResourceAsStream(path, in -> result.set(new String(ByteStreams.toByteArray(in), StandardCharsets.UTF_8)));
        return result.get();
    }

    @SneakyThrows
    public static Properties readResourceAsProperties(@NonNull String path) {
        val result = new Properties();
        withResourceAsStream(path, result::load);
        return result;
    }

    @SneakyThrows
    static void withResourceAsStream(String path, @NonNull IOExceptionThrowingConsumer<InputStream> consumer) {
        try (val in = ResourceReader.class.getResourceAsStream(path)) {
            if (in == null) {
                val message = String.format("%s. path=%s", NOT_FOUND_MESSAGE, path);
                throw new DataCloudJDBCException(message, SqlErrorCodes.UNDEFINED_FILE);
            }

            consumer.accept(in);
        } catch (IOException e) {
            val message = String.format("%s. path=%s", IO_EXCEPTION_MESSAGE, path);
            log.error(message, e);
            throw new DataCloudJDBCException(message, SqlErrorCodes.UNDEFINED_FILE, e);
        }
    }

    public static List<String> readResourceAsStringList(String path) {
        return Arrays.stream(readResourceAsString(path).split("\n"))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    private static final String NOT_FOUND_MESSAGE = "Resource file not found";
    private static final String IO_EXCEPTION_MESSAGE = "Error while loading resource file";

    @FunctionalInterface
    public static interface IOExceptionThrowingConsumer<T> {
        void accept(T t) throws IOException;
    }
}
