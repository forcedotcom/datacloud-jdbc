/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLFeatureNotSupportedException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class QueryJDBCAccessorTest {

    Calendar calendar = Calendar.getInstance();

    @Test
    public void shouldThrowUnsupportedError() {
        QueryJDBCAccessor absCls = Mockito.mock(QueryJDBCAccessor.class, Mockito.CALLS_REAL_METHODS);

        assertThrows(SQLFeatureNotSupportedException.class, absCls::getBytes);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getShort);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getInt);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getLong);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getFloat);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getDouble);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getBoolean);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getString);
        HashMap<String, Class<?>> stringDateHashMap = new HashMap<>();
        assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> absCls.getObject((Map<String, Class<?>>) stringDateHashMap));
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getByte);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getBigDecimal);
        assertThrows(SQLFeatureNotSupportedException.class, () -> absCls.getBigDecimal(1));
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getAsciiStream);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getUnicodeStream);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getBinaryStream);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getObject);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getCharacterStream);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getRef);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getBlob);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getClob);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getArray);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getStruct);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getURL);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getNClob);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getSQLXML);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getNString);
        assertThrows(SQLFeatureNotSupportedException.class, absCls::getNCharacterStream);
        assertThrows(SQLFeatureNotSupportedException.class, () -> absCls.getDate(calendar));
        assertThrows(SQLFeatureNotSupportedException.class, () -> absCls.getTime(calendar));
        assertThrows(SQLFeatureNotSupportedException.class, () -> absCls.getTimestamp(calendar));
    }
}
