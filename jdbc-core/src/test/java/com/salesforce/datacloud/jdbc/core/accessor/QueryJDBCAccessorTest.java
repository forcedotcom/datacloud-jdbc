/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc.core.accessor;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class QueryJDBCAccessorTest {

    Calendar calendar = Calendar.getInstance();

    @Test
    public void shouldThrowUnsupportedError() {
        QueryJDBCAccessor absCls = Mockito.mock(QueryJDBCAccessor.class, Mockito.CALLS_REAL_METHODS);

        val e1 = assertThrows(DataCloudJDBCException.class, absCls::getBytes);
        assertThat(e1).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e2 = assertThrows(DataCloudJDBCException.class, absCls::getShort);
        assertThat(e2).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e3 = assertThrows(DataCloudJDBCException.class, absCls::getInt);
        assertThat(e3).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e4 = assertThrows(DataCloudJDBCException.class, absCls::getLong);
        assertThat(e4).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e5 = assertThrows(DataCloudJDBCException.class, absCls::getFloat);
        assertThat(e5).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e6 = assertThrows(DataCloudJDBCException.class, absCls::getDouble);
        assertThat(e6).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e7 = assertThrows(DataCloudJDBCException.class, absCls::getBoolean);
        assertThat(e7).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e8 = assertThrows(DataCloudJDBCException.class, absCls::getString);
        assertThat(e8).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        HashMap<?, ?> stringDateHashMap = new HashMap<>();
        val e9 = assertThrows(
                DataCloudJDBCException.class, () -> absCls.getObject((Map<String, Class<?>>) stringDateHashMap));
        assertThat(e9).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e10 = assertThrows(DataCloudJDBCException.class, absCls::getByte);
        assertThat(e10).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e11 = assertThrows(DataCloudJDBCException.class, absCls::getBigDecimal);
        assertThat(e11).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e12 = assertThrows(DataCloudJDBCException.class, () -> absCls.getBigDecimal(1));
        assertThat(e12).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e13 = assertThrows(DataCloudJDBCException.class, absCls::getAsciiStream);
        assertThat(e13).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e14 = assertThrows(DataCloudJDBCException.class, absCls::getUnicodeStream);
        assertThat(e14).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e15 = assertThrows(DataCloudJDBCException.class, absCls::getBinaryStream);
        assertThat(e15).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e16 = assertThrows(DataCloudJDBCException.class, absCls::getObject);
        assertThat(e16).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e17 = assertThrows(DataCloudJDBCException.class, absCls::getCharacterStream);
        assertThat(e17).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e18 = assertThrows(DataCloudJDBCException.class, absCls::getRef);
        assertThat(e18).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e19 = assertThrows(DataCloudJDBCException.class, absCls::getBlob);
        assertThat(e19).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e20 = assertThrows(DataCloudJDBCException.class, absCls::getClob);
        assertThat(e20).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e21 = assertThrows(DataCloudJDBCException.class, absCls::getArray);
        assertThat(e21).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e22 = assertThrows(DataCloudJDBCException.class, absCls::getStruct);
        assertThat(e22).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e23 = assertThrows(DataCloudJDBCException.class, absCls::getURL);
        assertThat(e23).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e24 = assertThrows(DataCloudJDBCException.class, absCls::getNClob);
        assertThat(e24).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e25 = assertThrows(DataCloudJDBCException.class, absCls::getSQLXML);
        assertThat(e25).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e26 = assertThrows(DataCloudJDBCException.class, absCls::getNString);
        assertThat(e26).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e27 = assertThrows(DataCloudJDBCException.class, absCls::getNCharacterStream);
        assertThat(e27).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e28 = assertThrows(DataCloudJDBCException.class, () -> absCls.getDate(calendar));
        assertThat(e28).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e29 = assertThrows(DataCloudJDBCException.class, () -> absCls.getTime(calendar));
        assertThat(e29).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);

        val e30 = assertThrows(DataCloudJDBCException.class, () -> absCls.getTimestamp(calendar));
        assertThat(e30).hasRootCauseInstanceOf(SQLFeatureNotSupportedException.class);
    }
}
