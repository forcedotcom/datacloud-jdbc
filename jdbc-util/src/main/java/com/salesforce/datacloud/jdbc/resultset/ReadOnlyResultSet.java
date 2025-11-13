/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.resultset;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Mixin for a read-only result set.
 *
 * Overwrites all update-related methods to throw {@link SQLFeatureNotSupportedException}.
 * Used as a mixin to keep the boilerplate out of the actual result set implementations.
 */
interface ReadOnlyResultSet extends ResultSet {
    @Override
    public default int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public default boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowUpdated is not supported");
    }

    @Override
    public default boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowInserted is not supported");
    }

    @Override
    public default boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowDeleted is not supported");
    }

    @Override
    public default void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("refreshRow is not supported");
    }

    @Override
    public default void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob is not supported");
    }

    @Override
    public default void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public default void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }
}
