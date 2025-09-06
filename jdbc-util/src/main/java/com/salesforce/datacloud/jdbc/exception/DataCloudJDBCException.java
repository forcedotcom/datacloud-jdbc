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

    public DataCloudJDBCException() {
        super();
    }

    public DataCloudJDBCException(String reason) {
        super(reason);
    }

    public DataCloudJDBCException(String reason, String SQLState) {
        super(reason, SQLState);
    }

    public DataCloudJDBCException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    public DataCloudJDBCException(Throwable cause) {
        super(cause);
    }

    public DataCloudJDBCException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public DataCloudJDBCException(String reason, String SQLState, Throwable cause) {
        super(reason, SQLState, cause);
    }

    public DataCloudJDBCException(String reason, String SQLState, int vendorCode, Throwable cause) {
        super(reason, SQLState, vendorCode, cause);
    }

    public DataCloudJDBCException(
            String reason, String SQLState, String customerHint, String customerDetail, Throwable cause) {
        super(reason, SQLState, 0, cause);

        this.customerHint = customerHint;
        this.customerDetail = customerDetail;
    }
}
