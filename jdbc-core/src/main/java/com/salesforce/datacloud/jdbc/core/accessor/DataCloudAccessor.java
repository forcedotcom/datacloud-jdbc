/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * Project-owned replacement for Avatica's {@code Cursor.Accessor} interface. Provides the same
 * method signatures for column value access without any Avatica dependency.
 */
public interface DataCloudAccessor {

    boolean wasNull() throws SQLException;

    String getString() throws SQLException;

    boolean getBoolean() throws SQLException;

    byte getByte() throws SQLException;

    short getShort() throws SQLException;

    int getInt() throws SQLException;

    long getLong() throws SQLException;

    float getFloat() throws SQLException;

    double getDouble() throws SQLException;

    BigDecimal getBigDecimal() throws SQLException;

    BigDecimal getBigDecimal(int scale) throws SQLException;

    byte[] getBytes() throws SQLException;

    InputStream getAsciiStream() throws SQLException;

    InputStream getUnicodeStream() throws SQLException;

    InputStream getBinaryStream() throws SQLException;

    Object getObject() throws SQLException;

    Object getObject(Map<String, Class<?>> map) throws SQLException;

    <T> T getObject(Class<T> aClass) throws SQLException;

    Reader getCharacterStream() throws SQLException;

    String getNString() throws SQLException;

    Reader getNCharacterStream() throws SQLException;

    Ref getRef() throws SQLException;

    Blob getBlob() throws SQLException;

    Clob getClob() throws SQLException;

    Array getArray() throws SQLException;

    Struct getStruct() throws SQLException;

    NClob getNClob() throws SQLException;

    SQLXML getSQLXML() throws SQLException;

    Date getDate(Calendar calendar) throws SQLException;

    Time getTime(Calendar calendar) throws SQLException;

    Timestamp getTimestamp(Calendar calendar) throws SQLException;

    URL getURL() throws SQLException;
}
