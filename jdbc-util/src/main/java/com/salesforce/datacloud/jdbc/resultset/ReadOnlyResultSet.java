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
    default public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    default public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowUpdated is not supported");
    }

    @Override
    default public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowInserted is not supported");
    }

    @Override
    default public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowDeleted is not supported");
    }

    @Override
    default public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("refreshRow is not supported");
    }

    @Override
    default public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob is not supported");
    }

    @Override
    default public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    default public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }
}
