/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.salesforce.datacloud.jdbc.core.metadata.DataCloudResultSetMetaData;
import com.salesforce.datacloud.jdbc.core.resultset.SimpleResultSet;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataCloudMetadataResultSetTest {
    DataCloudMetadataResultSet dataCloudMetadataResultSet;

    @BeforeEach
    public void init() throws SQLException {
        dataCloudMetadataResultSet =
                DataCloudMetadataResultSet.of(new DataCloudResultSetMetaData(MetadataSchemas.COLUMNS), null);
    }

    @Test
    void getRow() throws SQLException {
        assertThat(dataCloudMetadataResultSet.getRow()).isEqualTo(0);

        dataCloudMetadataResultSet.close();
        assertThrows(SQLException.class, () -> dataCloudMetadataResultSet.next());
    }

    @Test
    void next() throws SQLException {
        dataCloudMetadataResultSet.close();
        assertThrows(SQLException.class, () -> dataCloudMetadataResultSet.next());
    }

    @Test
    void isClosed() throws SQLException {
        assertFalse(dataCloudMetadataResultSet.isClosed());
        dataCloudMetadataResultSet.close();
        assertTrue(dataCloudMetadataResultSet.isClosed());
    }

    @Test
    void getStatement() throws SQLException {
        assertThat(dataCloudMetadataResultSet.getStatement()).isNull();
    }

    @Test
    void unwrap() throws SQLException {
        assertThat(dataCloudMetadataResultSet.unwrap(ResultSetMetaData.class)).isNull();
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertThat(dataCloudMetadataResultSet.isWrapperFor(SimpleResultSet.class))
                .isFalse();
    }

    @Test
    void getHoldability() throws SQLException {
        assertThat(dataCloudMetadataResultSet.getHoldability()).isEqualTo(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test
    void getFetchSize() throws SQLException {
        assertThat(dataCloudMetadataResultSet.getFetchSize()).isEqualTo(0);
    }

    @Test
    void setFetchSize() {
        assertThrows(SQLFeatureNotSupportedException.class, () -> dataCloudMetadataResultSet.setFetchSize(0));
    }

    @SneakyThrows
    @Test
    void getWarnings() {
        assertThat((Iterable<? extends Throwable>) dataCloudMetadataResultSet.getWarnings())
                .isNull();
    }

    @Test
    void getConcurrency() throws SQLException {
        assertThat(dataCloudMetadataResultSet.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
    }

    @Test
    void getType() throws SQLException {
        assertThat(dataCloudMetadataResultSet.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
    }

    @Test
    void getFetchDirection() throws SQLException {
        assertThat(dataCloudMetadataResultSet.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
    }
}
