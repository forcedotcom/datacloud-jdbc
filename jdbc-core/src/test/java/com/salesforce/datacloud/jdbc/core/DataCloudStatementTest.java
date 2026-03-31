/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.grpcmock.junit5.InProcessGrpcMockExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;

@ExtendWith(InProcessGrpcMockExtension.class)
public class DataCloudStatementTest extends InterceptedHyperTestBase {
    @Mock
    private DataCloudConnection connection;

    static DataCloudStatement statement;

    @BeforeEach
    @SneakyThrows
    public void beforeEach() {
        connection = getInterceptedClientConnection();
        statement = new DataCloudStatement(connection);
    }

    private static Stream<Executable> unsupportedBatchExecutes() {
        return Stream.of(
                () -> statement.execute("", 1),
                () -> statement.execute("", new int[] {}),
                () -> statement.execute("", new String[] {}));
    }

    @ParameterizedTest
    @MethodSource("unsupportedBatchExecutes")
    @SneakyThrows
    public void batchExecutesAreNotSupported(Executable func) {
        val ex = Assertions.assertThrows(SQLException.class, func);
        assertThat(ex)
                .hasMessage("Batch execution is not supported in Data Cloud query")
                .hasFieldOrPropertyWithValue("SQLState", SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Test
    @SneakyThrows
    public void testResolveSessionTimeZoneWithInvalidTimezone() {
        Map<String, String> querySettings = new HashMap<>();
        querySettings.put("time_zone", "INVALID_TIMEZONE");
        statement.statementProperties =
                StatementProperties.builder().querySettings(querySettings).build();

        ZoneId result = statement.resolveSessionTimeZone();
        assertThat(result).isEqualTo(ZoneId.systemDefault());
    }

    @Test
    @SneakyThrows
    public void testResolveSessionTimeZoneWithValidTimezone() {
        Map<String, String> querySettings = new HashMap<>();
        querySettings.put("time_zone", "America/New_York");
        statement.statementProperties =
                StatementProperties.builder().querySettings(querySettings).build();

        ZoneId result = statement.resolveSessionTimeZone();
        assertThat(result).isEqualTo(ZoneId.of("America/New_York"));
    }

    @Test
    @SneakyThrows
    public void testResolveSessionTimeZoneWithEmptyTimezone() {
        Map<String, String> querySettings = new HashMap<>();
        querySettings.put("time_zone", "  ");
        statement.statementProperties =
                StatementProperties.builder().querySettings(querySettings).build();

        ZoneId result = statement.resolveSessionTimeZone();
        assertThat(result).isEqualTo(ZoneId.systemDefault());
    }

    @Test
    @SneakyThrows
    public void testResolveSessionTimeZoneWithNoTimezoneSetting() {
        statement.statementProperties =
                StatementProperties.builder().querySettings(new HashMap<>()).build();

        ZoneId result = statement.resolveSessionTimeZone();
        assertThat(result).isEqualTo(ZoneId.systemDefault());
    }
}
