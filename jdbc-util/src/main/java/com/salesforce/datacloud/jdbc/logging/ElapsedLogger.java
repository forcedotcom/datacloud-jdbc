/*
 * Copyright (c) 2024, Salesforce, Inc.
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
package com.salesforce.datacloud.jdbc.logging;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
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
            throws DataCloudJDBCException {
        val start = System.currentTimeMillis();
        try {
            logger.info("Starting name={}", name);
            val result = supplier.get();
            val elapsed = System.currentTimeMillis() - start;
            logger.info("Success name={}, millis={}, duration={}", name, elapsed, Duration.ofMillis(elapsed));
            return result;
        } catch (DataCloudJDBCException e) {
            val elapsed = System.currentTimeMillis() - start;
            logger.error("Failed name={}, millis={}, duration={}", name, elapsed, Duration.ofMillis(elapsed), e);
            throw e;
        } catch (SQLException e) {
            val elapsed = System.currentTimeMillis() - start;
            logger.error("Failed name={}, millis={}, duration={}", name, elapsed, Duration.ofMillis(elapsed), e);
            throw new DataCloudJDBCException(e);
        }
    }
}
