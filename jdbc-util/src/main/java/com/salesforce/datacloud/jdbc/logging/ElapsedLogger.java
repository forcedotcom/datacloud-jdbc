/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.logging;

import com.salesforce.datacloud.jdbc.util.ThrowingJdbcSupplier;
import java.sql.SQLException;
import java.time.Duration;
import lombok.val;
import org.slf4j.Logger;

public final class ElapsedLogger {
    private ElapsedLogger() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static <T> T logTimedValue(ThrowingJdbcSupplier<T> supplier, String name, Logger logger)
            throws SQLException {
        val start = System.currentTimeMillis();
        try {
            logger.info("Starting name={}", name);
            val result = supplier.get();
            val elapsed = System.currentTimeMillis() - start;
            logger.info("Success name={}, millis={}, duration={}", name, elapsed, Duration.ofMillis(elapsed));
            return result;
        } catch (SQLException e) {
            val elapsed = System.currentTimeMillis() - start;
            logger.error("Failed name={}, millis={}, duration={}", name, elapsed, Duration.ofMillis(elapsed), e);
            throw e;
        }
    }
}
