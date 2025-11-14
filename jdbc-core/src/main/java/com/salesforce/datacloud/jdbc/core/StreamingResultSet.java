/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.ArrowUtils.toColumnMetaData;

import com.salesforce.datacloud.jdbc.util.ThrowingJdbcSupplier;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.TimeZone;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;

@Slf4j
public class StreamingResultSet extends AvaticaResultSet implements DataCloudResultSet {
    @Getter
    private final String queryId;

    private final ArrowStreamReaderCursor cursor;
    ThrowingJdbcSupplier<QueryStatus> getQueryStatus;
    private final ColumnNameResolver columnNameResolver;

    private StreamingResultSet(
            ArrowStreamReaderCursor cursor,
            String queryId,
            AvaticaStatement statement,
            QueryState state,
            Meta.Signature signature,
            ResultSetMetaData resultSetMetaData,
            TimeZone timeZone,
            Meta.Frame firstFrame)
            throws SQLException {
        super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
        this.cursor = cursor;
        this.queryId = queryId;
        this.columnNameResolver = new ColumnNameResolver(signature.columns);
    }

    public static StreamingResultSet of(ArrowStreamReader resultStream, String queryId) throws SQLException {
        try {
            val schemaRoot = resultStream.getVectorSchemaRoot();
            val columns = toColumnMetaData(schemaRoot.getSchema().getFields());
            val timezone = TimeZone.getDefault();
            val state = new QueryState();
            val signature = new Meta.Signature(
                    columns, null, Collections.emptyList(), Collections.emptyMap(), null, Meta.StatementType.SELECT);
            val metadata = new AvaticaResultSetMetaData(null, null, signature);
            val cursor = new ArrowStreamReaderCursor(resultStream);
            val result = new StreamingResultSet(cursor, queryId, null, state, signature, metadata, timezone, null);
            result.execute2(cursor, columns);

            return result;
        } catch (IOException ex) {
            throw new SQLException("Unexpected error during ResultSet creation", "XX000", ex);
        }
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
    public int getRow() {
        return cursor.getRowsSeen();
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
