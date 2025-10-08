/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.exception;

import java.sql.SQLException;
import lombok.Getter;

@Getter
public class DataCloudJDBCException extends SQLException {
    private String customerHint;

    private String customerDetail;

    public DataCloudJDBCException(Throwable cause) {
        super(cause);
    }

    public DataCloudJDBCException(
            String reason, String SQLState, String customerHint, String customerDetail, Throwable cause) {
        super(reason, SQLState, 0, cause);

        this.customerHint = customerHint;
        this.customerDetail = customerDetail;
    }
}
