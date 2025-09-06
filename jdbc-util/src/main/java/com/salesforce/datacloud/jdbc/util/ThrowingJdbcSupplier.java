/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import java.sql.SQLException;

@FunctionalInterface
public interface ThrowingJdbcSupplier<T> {
    T get() throws SQLException;
}
