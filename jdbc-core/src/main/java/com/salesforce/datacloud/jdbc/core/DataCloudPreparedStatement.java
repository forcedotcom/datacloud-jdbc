/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import com.salesforce.datacloud.jdbc.protocol.data.ParameterAccumulator;
import com.salesforce.datacloud.jdbc.protocol.data.ParameterBinding;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import salesforce.cdp.hyperdb.v1.QueryParam;

/** JDBC {@link PreparedStatement} implementation for positional {@code ?} parameters. */
public class DataCloudPreparedStatement extends DataCloudPreparedStatementBase<Integer> implements PreparedStatement {
    // Package-private for tests that need to inspect bound parameters.
    final ParameterAccumulator parameters = new ParameterAccumulator();
    private final ProvidedParameters providedParameters = new ProvidedParameters(
            QueryParam.ParameterStyle.QUESTION_MARK, new PositionalParameterEntries(parameters.getParameters()));

    DataCloudPreparedStatement(DataCloudConnection connection, String sql) {
        super(connection, sql);
    }

    @Override
    protected void bindParameter(Integer parameterIndex, HyperType type, Object value) throws SQLException {
        try {
            parameters.setParameter(parameterIndex, type, value);
        } catch (IllegalArgumentException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    @Override
    protected ProvidedParameters getProvidedParameters() {
        return providedParameters;
    }

    @Override
    public void clearParameters() {
        parameters.clearParameters();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setNullParameter(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean value) throws SQLException {
        setBooleanParameter(parameterIndex, value);
    }

    @Override
    public void setByte(int parameterIndex, byte value) throws SQLException {
        setByteParameter(parameterIndex, value);
    }

    @Override
    public void setShort(int parameterIndex, short value) throws SQLException {
        setShortParameter(parameterIndex, value);
    }

    @Override
    public void setInt(int parameterIndex, int value) throws SQLException {
        setIntParameter(parameterIndex, value);
    }

    @Override
    public void setLong(int parameterIndex, long value) throws SQLException {
        setLongParameter(parameterIndex, value);
    }

    @Override
    public void setFloat(int parameterIndex, float value) throws SQLException {
        setFloatParameter(parameterIndex, value);
    }

    @Override
    public void setDouble(int parameterIndex, double value) throws SQLException {
        setDoubleParameter(parameterIndex, value);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal value) throws SQLException {
        setBigDecimalParameter(parameterIndex, value);
    }

    @Override
    public void setString(int parameterIndex, String value) throws SQLException {
        setStringParameter(parameterIndex, value);
    }

    @Override
    public void setDate(int parameterIndex, Date value) throws SQLException {
        setDateParameter(parameterIndex, value);
    }

    @Override
    public void setTime(int parameterIndex, Time value) throws SQLException {
        setTimeParameter(parameterIndex, value);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp value) throws SQLException {
        setTimestampParameter(parameterIndex, value);
    }

    @Override
    public void setObject(int parameterIndex, Object value) throws SQLException {
        setObjectParameter(parameterIndex, value);
    }

    @Override
    public void setObject(int parameterIndex, Object value, int targetSqlType) throws SQLException {
        setObjectParameter(parameterIndex, value, targetSqlType);
    }

    @Override
    public void setDate(int parameterIndex, Date value, Calendar calendar) throws SQLException {
        setDateParameter(parameterIndex, value, calendar);
    }

    @Override
    public void setTime(int parameterIndex, Time value, Calendar calendar) throws SQLException {
        setTimeParameter(parameterIndex, value, calendar);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp value, Calendar calendar) throws SQLException {
        setTimestampParameter(parameterIndex, value, calendar);
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] value) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream value, int length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream value, int length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream value, int length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setRef(int parameterIndex, Ref value) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setBlob(int parameterIndex, Blob value) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setClob(int parameterIndex, Clob value) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setArray(int parameterIndex, Array value) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setURL(int parameterIndex, URL value) throws SQLException {
        throw unsupported();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw unsupported();
    }

    @Override
    public void setRowId(int parameterIndex, RowId value) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML value) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setObject(int parameterIndex, Object value, int targetSqlType, int scaleOrLength) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream value, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream value, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream value) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream value) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw unsupported();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw unsupported();
    }

    private static SQLException unsupported() {
        return new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    private static final class PositionalParameterEntries extends AbstractList<Map.Entry<String, ParameterBinding>> {
        private final List<ParameterBinding> bindings;

        private PositionalParameterEntries(List<ParameterBinding> bindings) {
            this.bindings = bindings;
        }

        @Override
        public Map.Entry<String, ParameterBinding> get(int index) {
            return new AbstractMap.SimpleImmutableEntry<>(String.valueOf(index + 1), bindings.get(index));
        }

        @Override
        public int size() {
            return bindings.size();
        }
    }
}
