/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.resultset;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SimpleResultSetTest {

    @Test
    public void shouldThrowUnsupportedError() {
        SimpleResultSet resultSet = Mockito.mock(SimpleResultSet.class, Mockito.CALLS_REAL_METHODS);

        // Test methods from SimpleResultSet that throw SQLFeatureNotSupportedException
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::getCursorName);
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBlob(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getClob(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRef(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getURL(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRowId(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getSQLXML(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNString(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNCharacterStream(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getAsciiStream(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getUnicodeStream(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBinaryStream(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getCharacterStream(1));

        // Test un-implemented methods from SimpleResultSet that throw UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getDate(1));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getTimestamp(1));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getTime(1));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getDate(1, null));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getTimestamp(1, null));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getTime(1, null));

        // Test methods from ForwardOnlyResultSet interface
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.setFetchDirection(ResultSet.FETCH_REVERSE));
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::isBeforeFirst);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::isAfterLast);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::isFirst);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::isLast);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::first);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::last);
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.absolute(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.relative(1));
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::previous);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::beforeFirst);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::afterLast);

        // Test methods from ReadOnlyResultSet interface
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::rowUpdated);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::rowInserted);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::rowDeleted);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::insertRow);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::updateRow);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::deleteRow);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::cancelRowUpdates);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::refreshRow);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::moveToInsertRow);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::moveToCurrentRow);
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNull(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBoolean(1, true));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateByte(1, (byte) 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateShort(1, (short) 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateInt(1, 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateLong(1, 1L));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateFloat(1, 1.0f));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDouble(1, 1.0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBigDecimal(1, BigDecimal.ONE));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateString(1, "test"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBytes(1, new byte[0]));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDate(1, new java.sql.Date(0)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTime(1, new java.sql.Time(0)));
        assertThrows(
                SQLFeatureNotSupportedException.class, () -> resultSet.updateTimestamp(1, new java.sql.Timestamp(0)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateAsciiStream(1, Mockito.mock(InputStream.class), 1));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateBinaryStream(1, Mockito.mock(InputStream.class), 1));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateCharacterStream(1, Mockito.mock(Reader.class), 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject(1, new Object(), 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject(1, new Object()));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNull("col"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBoolean("col", true));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateByte("col", (byte) 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateShort("col", (short) 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateInt("col", 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateLong("col", 1L));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateFloat("col", 1.0f));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDouble("col", 1.0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBigDecimal("col", BigDecimal.ONE));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateString("col", "test"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBytes("col", new byte[0]));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDate("col", new java.sql.Date(0)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTime("col", new java.sql.Time(0)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateTimestamp("col", new java.sql.Timestamp(0)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateAsciiStream("col", Mockito.mock(InputStream.class), 1));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateBinaryStream("col", Mockito.mock(InputStream.class), 1));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateCharacterStream("col", Mockito.mock(Reader.class), 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject("col", new Object(), 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject("col", new Object()));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRef(1, Mockito.mock(Ref.class)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRef("col", Mockito.mock(Ref.class)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, Mockito.mock(Blob.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob("col", Mockito.mock(Blob.class)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, Mockito.mock(Clob.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class, () -> resultSet.updateClob("col", Mockito.mock(Clob.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateArray(1, Mockito.mock(java.sql.Array.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateArray("col", Mockito.mock(java.sql.Array.class)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRowId(1, Mockito.mock(RowId.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class, () -> resultSet.updateRowId("col", Mockito.mock(RowId.class)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNString(1, "test"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNString("col", "test"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, Mockito.mock(NClob.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob("col", Mockito.mock(NClob.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class, () -> resultSet.updateSQLXML(1, Mockito.mock(SQLXML.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class, () -> resultSet.updateSQLXML("col", Mockito.mock(SQLXML.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateNCharacterStream(1, Mockito.mock(Reader.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateNCharacterStream("col", Mockito.mock(Reader.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateAsciiStream(1, Mockito.mock(InputStream.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateBinaryStream(1, Mockito.mock(InputStream.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateCharacterStream(1, Mockito.mock(Reader.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateAsciiStream("col", Mockito.mock(InputStream.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateBinaryStream("col", Mockito.mock(InputStream.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateCharacterStream("col", Mockito.mock(Reader.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateBlob(1, Mockito.mock(InputStream.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateBlob("col", Mockito.mock(InputStream.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, Mockito.mock(Reader.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateClob("col", Mockito.mock(Reader.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, Mockito.mock(Reader.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateNClob("col", Mockito.mock(Reader.class), 1L));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateNCharacterStream(1, Mockito.mock(Reader.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateNCharacterStream("col", Mockito.mock(Reader.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateAsciiStream(1, Mockito.mock(InputStream.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateBinaryStream(1, Mockito.mock(InputStream.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateCharacterStream(1, Mockito.mock(Reader.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateAsciiStream("col", Mockito.mock(InputStream.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateBinaryStream("col", Mockito.mock(InputStream.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateCharacterStream("col", Mockito.mock(Reader.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, Mockito.mock(InputStream.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> resultSet.updateBlob("col", Mockito.mock(InputStream.class)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, Mockito.mock(Reader.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class, () -> resultSet.updateClob("col", Mockito.mock(Reader.class)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, Mockito.mock(Reader.class)));
        assertThrows(
                SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob("col", Mockito.mock(Reader.class)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNClob(1));
    }
}
