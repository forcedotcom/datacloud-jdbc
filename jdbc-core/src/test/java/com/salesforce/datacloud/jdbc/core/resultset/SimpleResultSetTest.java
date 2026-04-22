/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.resultset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.datacloud.jdbc.core.DataCloudMetadataResultSet;
import com.salesforce.datacloud.jdbc.core.MetadataSchemas;
import com.salesforce.datacloud.jdbc.core.metadata.DataCloudResultSetMetaData;
import com.salesforce.datacloud.jdbc.protocol.data.ColumnMetadata;
import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

class SimpleResultSetTest {

    @Test
    public void shouldThrowUnsupportedError() {
        SimpleResultSet resultSet = Mockito.mock(SimpleResultSet.class, Mockito.CALLS_REAL_METHODS);

        // Test methods from SimpleResultSet that throw SQLFeatureNotSupportedException
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::clearWarnings);
        assertThrows(SQLFeatureNotSupportedException.class, resultSet::getCursorName);
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBlob(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getClob(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNClob(1));
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
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getBigDecimal(1, 1));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getBytes(1));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getArray(1));

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
    }

    @Test
    void wasNullReflectsNullAndNonNullColumnValues() throws SQLException {
        List<Object> data = Arrays.asList(Collections.singletonList("TABLE"), Collections.singletonList(null));
        SimpleResultSet<?> resultSet =
                DataCloudMetadataResultSet.of(new DataCloudResultSetMetaData(MetadataSchemas.TABLE_TYPES), data);

        assertTrue(resultSet.next());
        assertEquals("TABLE", resultSet.getString(1));
        assertFalse(resultSet.wasNull());

        assertTrue(resultSet.next());
        assertNull(resultSet.getString(1));
        assertTrue(resultSet.wasNull());
    }

    /**
     * Covers branches in SimpleMetadataResultSet's private getValue (invoked via getString/getInt
     * etc.): closed result set, no current row, row index out of bounds, row not a List, column
     * index out of bounds for row.
     */
    @Test
    void simpleMetadataResultSetStatusValue() throws SQLException {
        // 1. ResultSet is closed
        SimpleResultSet<?> rs = DataCloudMetadataResultSet.of(
                new DataCloudResultSetMetaData(MetadataSchemas.TABLE_TYPES),
                Arrays.asList(Collections.singletonList("x")));
        rs.close();
        SQLException ex = assertThrows(SQLException.class, () -> rs.getString(1));
        assertTrue(ex.getMessage().contains("ResultSet is closed"), "message: " + ex.getMessage());

        // 2. No current row (get before next())
        SimpleResultSet<?> rs2 = DataCloudMetadataResultSet.of(
                new DataCloudResultSetMetaData(MetadataSchemas.TABLE_TYPES),
                Arrays.asList(Collections.singletonList("x")));
        ex = assertThrows(SQLException.class, () -> rs2.getString(1));
        assertTrue(ex.getMessage().contains("No current row"), "message: " + ex.getMessage());

        // 3. Row index out of bounds (past last row)
        SimpleResultSet<?> rs3 = DataCloudMetadataResultSet.of(
                new DataCloudResultSetMetaData(MetadataSchemas.TABLE_TYPES),
                Arrays.asList(Collections.singletonList("x")));
        assertTrue(rs3.next());
        assertFalse(rs3.next());
        ex = assertThrows(SQLException.class, () -> rs3.getString(1));
        assertTrue(ex.getMessage().contains("Row index out of bounds"), "message: " + ex.getMessage());

        // 4. Data row is not a List
        SimpleResultSet<?> rs4 = DataCloudMetadataResultSet.of(
                new DataCloudResultSetMetaData(MetadataSchemas.TABLE_TYPES), Arrays.asList(123));
        assertTrue(rs4.next());
        ex = assertThrows(SQLException.class, () -> rs4.getString(1));
        assertTrue(ex.getMessage().contains("Data row is not a List"), "message: " + ex.getMessage());

        // 5. Column index out of bounds for row (row has fewer columns than requested)
        SimpleResultSet<?> rs5 = DataCloudMetadataResultSet.of(
                new DataCloudResultSetMetaData(MetadataSchemas.COLUMNS),
                Arrays.asList(Collections.singletonList("only one")));
        assertTrue(rs5.next());
        ex = assertThrows(SQLException.class, () -> rs5.getString(5));
        assertTrue(ex.getMessage().contains("out of bounds for row"), "message: " + ex.getMessage());
    }

    /**
     * Covers the out-of-range branches in SimpleResultSet for getByte, getShort, and getInt: when
     * the long value is outside the target type's range, the getter throws SQLException. (getFloat
     * uses getDouble and would need a double column to trigger out-of-range; current metadata has
     * only integer/varchar columns.)
     */
    @Test
    void getNumericTypesThrowWhenValueOutOfRange() throws SQLException {
        // GET_COLUMNS: column 5 (1-based) is DATA_TYPE (INTEGER)
        int col = 5;

        SimpleResultSet<?> resultSet = DataCloudMetadataResultSet.of(
                new DataCloudResultSetMetaData(MetadataSchemas.COLUMNS),
                Arrays.asList(
                        rowWithLongAt(col, (long) Byte.MAX_VALUE + 1),
                        rowWithLongAt(col, (long) Byte.MIN_VALUE - 1),
                        rowWithLongAt(col, (long) Short.MAX_VALUE + 1),
                        rowWithLongAt(col, (long) Short.MIN_VALUE - 1),
                        rowWithLongAt(col, (long) Integer.MAX_VALUE + 1),
                        rowWithLongAt(col, (long) Integer.MIN_VALUE - 1)));

        // getByte: above and below byte range
        assertTrue(resultSet.next());
        assertThrowsOutOfRange("byte", () -> resultSet.getByte(col));
        assertTrue(resultSet.next());
        assertThrowsOutOfRange("byte", () -> resultSet.getByte(col));

        // getShort: above and below short range
        assertTrue(resultSet.next());
        assertThrowsOutOfRange("short", () -> resultSet.getShort(col));
        assertTrue(resultSet.next());
        assertThrowsOutOfRange("short", () -> resultSet.getShort(col));

        // getInt: above and below int range
        assertTrue(resultSet.next());
        assertThrowsOutOfRange("int", () -> resultSet.getInt(col));
        assertTrue(resultSet.next());
        assertThrowsOutOfRange("int", () -> resultSet.getInt(col));

        assertFalse(resultSet.next());
    }

    private static List<Object> rowWithLongAt(int oneBasedColumnIndex, long value) {
        List<Object> row = new ArrayList<>(Collections.nCopies(24, null));
        row.set(oneBasedColumnIndex - 1, value);
        return row;
    }

    private static void assertThrowsOutOfRange(String typeName, Executable executable) {
        SQLException ex = assertThrows(SQLException.class, executable);
        String suffix = "byte".equals(typeName) || "short".equals(typeName) ? "a " + typeName : "an " + typeName;
        assertTrue(ex.getMessage().contains("out of range for " + suffix), "message: " + ex.getMessage());
    }

    /**
     * Exercises the label-based getters from {@link ResultSetWithPositionalGetters} so that the
     * interface default methods (which delegate via findColumn) are covered. Uses a real
     * {@link DataCloudMetadataResultSet} (not a mock) so JaCoCo attributes execution to the
     * interface and Codecov reports coverage for ResultSetWithPositionalGetters.
     */
    @Test
    void resultSetWithPositionalGettersDelegateAndThrow() throws SQLException {
        ResultSetWithPositionalGetters resultSet =
                DataCloudMetadataResultSet.of(new DataCloudResultSetMetaData(MetadataSchemas.TABLE_TYPES), null);
        String col = "TABLE_TYPE"; // single column from GET_TABLE_TYPES

        // UnsupportedOperationException from SimpleResultSet positional implementations
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getBytes(col));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getDate(col));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getDate(col, (Calendar) null));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getTime(col));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getTime(col, (Calendar) null));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getTimestamp(col));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getTimestamp(col, (Calendar) null));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getBigDecimal(col, 1));
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getArray(col));

        // SQLFeatureNotSupportedException from SimpleResultSet positional implementations
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getAsciiStream(col));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getUnicodeStream(col));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBinaryStream(col));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getCharacterStream(col));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRef(col));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBlob(col));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getClob(col));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getURL(col));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRowId(col));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNClob(col));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getSQLXML(col));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNString(col));
        assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNCharacterStream(col));

        // getObject(String, Map) -> UnsupportedOperationException (non-empty map)
        Map<String, Class<?>> map = Collections.singletonMap("key", String.class);
        assertThrows(UnsupportedOperationException.class, () -> resultSet.getObject(col, map));

        // No current row: delegate to positional getters which throw SQLException
        assertThrows(SQLException.class, () -> resultSet.getObject(col));
        assertThrows(SQLException.class, () -> resultSet.getObject(col, String.class));
        assertThrows(SQLException.class, () -> resultSet.getBigDecimal(col));
    }

    @Test
    void tinyintColumnSupportsGetLongGetDoubleGetBigDecimal() throws SQLException {
        List<ColumnMetadata> schema =
                Collections.singletonList(new ColumnMetadata("val", HyperType.int8(true), "TINYINT"));
        SimpleResultSet<?> rs = DataCloudMetadataResultSet.of(
                new DataCloudResultSetMetaData(schema), Arrays.asList(Collections.singletonList(42L)));

        assertTrue(rs.next());
        assertEquals(42L, rs.getLong(1));
        assertFalse(rs.wasNull());
    }

    @Test
    void tinyintColumnSupportsGetDouble() throws SQLException {
        List<ColumnMetadata> schema =
                Collections.singletonList(new ColumnMetadata("val", HyperType.int8(true), "TINYINT"));
        SimpleResultSet<?> rs = DataCloudMetadataResultSet.of(
                new DataCloudResultSetMetaData(schema), Arrays.asList(Collections.singletonList(7L)));

        assertTrue(rs.next());
        assertEquals(7.0, rs.getDouble(1));
    }

    @Test
    void tinyintColumnSupportsGetBigDecimal() throws SQLException {
        List<ColumnMetadata> schema =
                Collections.singletonList(new ColumnMetadata("val", HyperType.int8(true), "TINYINT"));
        SimpleResultSet<?> rs = DataCloudMetadataResultSet.of(
                new DataCloudResultSetMetaData(schema), Arrays.asList(Collections.singletonList(99L)));

        assertTrue(rs.next());
        assertEquals(new BigDecimal(99), rs.getBigDecimal(1));
    }

    @Test
    void getFloatAcceptsNegativeValuesWithinRange() throws SQLException {
        int col = 5;
        SimpleResultSet<?> rs = DataCloudMetadataResultSet.of(
                new DataCloudResultSetMetaData(MetadataSchemas.COLUMNS), Arrays.asList(rowWithLongAt(col, -42L)));

        assertTrue(rs.next());
        assertEquals(-42.0f, rs.getFloat(col));
    }
}
