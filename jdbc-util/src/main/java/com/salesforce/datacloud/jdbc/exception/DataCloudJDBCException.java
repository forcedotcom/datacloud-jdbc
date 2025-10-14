/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.exception;

import java.sql.SQLException;
import lombok.Getter;

@Getter
public class DataCloudJDBCException extends SQLException {
    /**
     * The fully formatted error message including customer detail and hint (while the normal exception message might not contain those based of the
     * `errorsIncludeCustomerDetails` property).
     */
    private final String fullCustomerMessage;

    /**
     * The fully formatted error message including system detail. This is the message that should be logged in a cloud
     * service setting.
     */
    private final String fullSystemMessage;

    /**
     * The primary (terse) error message (without TraceId addition)
     */
    private final String primaryMessage;

    /**
     * A suggestion on what to do about the problem. Differs from customer_detail by offering advise rather than hard facts.
     * Can be returned to the customer but in a cloud scenario where the query is coming from a third party likely
     * shouldn't be logged. Only makes sense to show to the user, if the user can actually change the SQL query.
     * Otherwise, this hint would probably not be actionable to the user.
     */
    private final String customerHint;

    /**
     * Error detail with data that is might be sensitive to the customer and thus shouldn't be logged in cloud services.
     */
    private final String customerDetail;

    /**
     * Error detail with system data classification. In the case of cloud service this might provide internal details
     * about the server side failure (like missing access to system files) that should not be exposed to the customer.
     */
    private final String systemDetail;

    public DataCloudJDBCException(
            String reason,
            String fullCustomerMessage,
            String fullSystemMessage,
            String SQLState,
            String primaryMessage,
            String customerHint,
            String customerDetail,
            String systemDetail,
            Throwable cause) {
        super(reason, SQLState, cause);

        this.fullCustomerMessage = fullCustomerMessage;
        this.fullSystemMessage = fullSystemMessage;
        this.primaryMessage = primaryMessage;
        this.customerHint = customerHint;
        this.customerDetail = customerDetail;
        this.systemDetail = systemDetail;
    }
}
