/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;
import lombok.val;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;

public class MetadataResultSet extends AvaticaResultSet {
    private final ColumnNameResolver columnNameResolver;

    public MetadataResultSet(
            AvaticaStatement statement,
            QueryState state,
            Meta.Signature signature,
            ResultSetMetaData resultSetMetaData,
            TimeZone timeZone,
            Meta.Frame firstFrame)
            throws SQLException {
        super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
        this.columnNameResolver = new ColumnNameResolver(signature.columns);
    }

    public static AvaticaResultSet of() throws SQLException {
        val signature = new Meta.Signature(
                ImmutableList.of(), null, ImmutableList.of(), ImmutableMap.of(), null, Meta.StatementType.SELECT);
        return of(
                null,
                new QueryState(),
                signature,
                new AvaticaResultSetMetaData(null, null, signature),
                TimeZone.getDefault(),
                null,
                ImmutableList.of());
    }

    public static AvaticaResultSet of(
            AvaticaStatement statement,
            QueryState state,
            Meta.Signature signature,
            ResultSetMetaData resultSetMetaData,
            TimeZone timeZone,
            Meta.Frame firstFrame,
            List<Object> data)
            throws SQLException {
        AvaticaResultSet result;
        result = new MetadataResultSet(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
        result.execute2(new MetadataCursor(data), signature.columns);
        return result;
    }

    @Override
    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return columnNameResolver.findColumn(columnLabel);
    }

    /**
     * Override getter methods that take String columnLabel to ensure they use our optimized findColumn() method.
     * This is necessary because Avatica's implementation might use a private method or cache.
     */
    @Override
    public String getString(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getString(columnIndex);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getInt(columnIndex);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getLong(columnIndex);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBoolean(columnIndex);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getByte(columnIndex);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getShort(columnIndex);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getFloat(columnIndex);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDouble(columnIndex);
    }
}
