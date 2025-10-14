/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.partial;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.sql.SQLException;
import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Note that these tests do not use Statement::executeQuery which attempts to iterate immediately,
 * getQueryResult is not resilient to server timeout, only getQueryInfo.
 */
@Slf4j
@ExtendWith(LocalHyperTestBase.class)
class DataCloudQueryPollingFunctionalTest {
    Duration small = Duration.ofSeconds(5);

    @SneakyThrows
    @Test
    void throwsAboutNotEnoughRows_disallowLessThan() {
        try (val connection = getHyperQueryConnection()) {
            val statement = connection.createStatement().unwrap(DataCloudStatement.class);

            statement.execute("select * from generate_series(1, 109)");

            assertThat(connection
                            .waitFor(statement.getQueryId(), small, QueryStatus::allResultsProduced)
                            .allResultsProduced())
                    .isTrue();

            Assertions.assertThatThrownBy(() -> connection.waitFor(
                            statement.getQueryId(), Duration.ofSeconds(1), t -> t.getRowCount() >= 110))
                    .hasMessageContaining("Predicate was not satisfied when execution finished.")
                    .isInstanceOf(SQLException.class);
        }
    }
}
