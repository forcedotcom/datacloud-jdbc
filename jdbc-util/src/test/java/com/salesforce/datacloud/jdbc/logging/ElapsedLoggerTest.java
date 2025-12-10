/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.salesforce.datacloud.jdbc.util.ThrowingJdbcSupplier;
import java.sql.SQLException;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class ElapsedLoggerTest {

    @Test
    void logTimedValueShouldReturnResultAndLogSuccess() throws SQLException {
        val logger = mock(Logger.class);
        val expectedResult = "test-result";
        ThrowingJdbcSupplier<String> supplier = () -> expectedResult;
        val operationName = "test-operation";

        val result = ElapsedLogger.logTimedValue(supplier, operationName, logger);

        assertThat(result).isEqualTo(expectedResult);
        verify(logger, times(1)).info(eq("Starting name={}"), eq(operationName));
        verify(logger, times(1))
                .info(eq("Success name={}, millis={}, duration={}"), eq(operationName), anyLong(), any());
    }

    @Test
    void logTimedValueShouldLogErrorAndRethrowException() {
        val logger = mock(Logger.class);
        val expectedException = new SQLException("test-error");
        ThrowingJdbcSupplier<String> supplier = () -> {
            throw expectedException;
        };
        val operationName = "failing-operation";

        assertThatThrownBy(() -> ElapsedLogger.logTimedValue(supplier, operationName, logger))
                .isEqualTo(expectedException);

        verify(logger, times(1)).info(eq("Starting name={}"), eq(operationName));
        verify(logger, times(1))
                .info(
                        eq("Failed name={}, millis={}, duration={}"),
                        eq(operationName),
                        anyLong(),
                        any(),
                        eq(expectedException));
    }
}
