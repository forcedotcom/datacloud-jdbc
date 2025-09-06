/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class QueryJDBCDataCursorTest {

    static MetadataCursor cursor;

    @AfterEach
    public void tearDown() {
        cursor.close();
    }

    @Test
    public void testCursorNextAndRowCount() {
        List<Object> data = new ArrayList<>();
        data.add("Account_home_dll");
        cursor = new MetadataCursor(data);
        cursor.next();
        assertThat(cursor.next()).isFalse();
    }
}
