/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import com.salesforce.datacloud.jdbc.protocol.data.ParameterBinding;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import salesforce.cdp.hyperdb.v1.QueryParam;

/**
 * A Hyper specific prepared statement for named parameters such as {@code :account_id}.
 * Create instances with {@link DataCloudConnection#prepareNamedStatement(String)} and execute the
 * SQL supplied there using {@link #executeQuery()}, {@link #execute()}, or {@link
 * #executeAsyncQuery()}.
 *
 * <p>Setter names are the parameter names without the leading colon. For example,
 * {@code setInt("account_id", 42)} binds {@code :account_id}. Any non-empty name is accepted,
 * including names used as quoted parameters such as {@code :"order total"}; use {@code "order
 * total"} in the setter. The same binding supplies every occurrence of a name in the SQL.
 *
 * <p>Setting an existing name again replaces its type and value. Binding order is not significant.
 * {@link #clearParameters()} removes all bindings; executing with no bindings is allowed. SQL
 * {@code NULL} values follow {@link
 * DataCloudPreparedStatement}: {@link #setObject(String, Object, int)} with a null value binds an
 * untyped null, while type-specific setters bind a null of their corresponding type.
 *
 * <p>This class exposes only the named setters supported by the driver. It is not a {@link
 * java.sql.PreparedStatement}, and {@link java.sql.DatabaseMetaData#supportsNamedParameters()}
 * remains false because that JDBC capability specifically describes named {@link
 * java.sql.CallableStatement} parameters. Like JDBC statements generally, instances are not
 * thread-safe.
 */
public final class DataCloudNamedPreparedStatement extends DataCloudPreparedStatementBase<String> {
    private static final String PARAMETER_NAME_ERROR = "Parameter name must not be null or empty";

    final Map<String, ParameterBinding> parameters = new HashMap<>();
    private final ProvidedParameters providedParameters =
            new ProvidedParameters(QueryParam.ParameterStyle.NAMED, parameters.entrySet());

    DataCloudNamedPreparedStatement(DataCloudConnection connection, String sql) {
        super(connection, sql);
    }

    @Override
    protected void bindParameter(String parameterName, HyperType type, Object value) throws SQLException {
        if (parameterName == null || parameterName.isEmpty()) {
            throw new SQLException(PARAMETER_NAME_ERROR);
        }
        parameters.put(parameterName, new ParameterBinding(type, value));
    }

    @Override
    protected ProvidedParameters getProvidedParameters() {
        return providedParameters;
    }

    @Override
    public void clearParameters() {
        parameters.clear();
    }

    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNullParameter(parameterName, sqlType);
    }

    public void setBoolean(String parameterName, boolean value) throws SQLException {
        setBooleanParameter(parameterName, value);
    }

    public void setByte(String parameterName, byte value) throws SQLException {
        setByteParameter(parameterName, value);
    }

    public void setShort(String parameterName, short value) throws SQLException {
        setShortParameter(parameterName, value);
    }

    public void setInt(String parameterName, int value) throws SQLException {
        setIntParameter(parameterName, value);
    }

    public void setLong(String parameterName, long value) throws SQLException {
        setLongParameter(parameterName, value);
    }

    public void setFloat(String parameterName, float value) throws SQLException {
        setFloatParameter(parameterName, value);
    }

    public void setDouble(String parameterName, double value) throws SQLException {
        setDoubleParameter(parameterName, value);
    }

    public void setBigDecimal(String parameterName, BigDecimal value) throws SQLException {
        setBigDecimalParameter(parameterName, value);
    }

    public void setString(String parameterName, String value) throws SQLException {
        setStringParameter(parameterName, value);
    }

    public void setDate(String parameterName, Date value) throws SQLException {
        setDateParameter(parameterName, value);
    }

    public void setTime(String parameterName, Time value) throws SQLException {
        setTimeParameter(parameterName, value);
    }

    public void setTimestamp(String parameterName, Timestamp value) throws SQLException {
        setTimestampParameter(parameterName, value);
    }

    public void setObject(String parameterName, Object value) throws SQLException {
        setObjectParameter(parameterName, value);
    }

    public void setObject(String parameterName, Object value, int targetSqlType) throws SQLException {
        setObjectParameter(parameterName, value, targetSqlType);
    }

    public void setDate(String parameterName, Date value, Calendar calendar) throws SQLException {
        setDateParameter(parameterName, value, calendar);
    }

    public void setTime(String parameterName, Time value, Calendar calendar) throws SQLException {
        setTimeParameter(parameterName, value, calendar);
    }

    public void setTimestamp(String parameterName, Timestamp value, Calendar calendar) throws SQLException {
        setTimestampParameter(parameterName, value, calendar);
    }
}
