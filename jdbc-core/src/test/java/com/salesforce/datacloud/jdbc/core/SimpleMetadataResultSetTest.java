/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.salesforce.datacloud.jdbc.core.resultset.SimpleResultSet;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleMetadataResultSetTest {
    SimpleMetadataResultSet simpleMetadataResultSet;

    QueryDBMetadata queryDBMetadata = QueryDBMetadata.GET_COLUMNS;

    @BeforeEach
    public void init() throws SQLException {
        simpleMetadataResultSet = SimpleMetadataResultSet.of(queryDBMetadata, null);
    }

    @Test
    void getRow() throws SQLException {
        assertThat(simpleMetadataResultSet.getRow()).isEqualTo(0);

        simpleMetadataResultSet.close();
        assertThrows(SQLException.class, () -> simpleMetadataResultSet.next());
    }

    @Test
    void next() throws SQLException {
        simpleMetadataResultSet.close();
        assertThrows(SQLException.class, () -> simpleMetadataResultSet.next());
    }

    @Test
    void isClosed() throws SQLException {
        assertFalse(simpleMetadataResultSet.isClosed());
        simpleMetadataResultSet.close();
        assertTrue(simpleMetadataResultSet.isClosed());
    }

    @Test
    void getStatement() throws SQLException {
        assertThat(simpleMetadataResultSet.getStatement()).isNull();
    }

    @Test
    void unwrap() throws SQLException {
        assertThat(simpleMetadataResultSet.unwrap(ResultSetMetaData.class)).isNull();
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertThat(simpleMetadataResultSet.isWrapperFor(SimpleResultSet.class)).isFalse();
    }

    @Test
    void getHoldability() throws SQLException {
        assertThat(simpleMetadataResultSet.getHoldability()).isEqualTo(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test
    void getFetchSize() throws SQLException {
        assertThat(simpleMetadataResultSet.getFetchSize()).isEqualTo(0);
    }

    @SneakyThrows
    @Test
    void getWarnings() {
        assertThat((Iterable<? extends Throwable>) simpleMetadataResultSet.getWarnings())
                .isNull();
    }
}
