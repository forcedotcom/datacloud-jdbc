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
package com.salesforce.datacloud.jdbc.exception;

import java.sql.SQLException;
import lombok.Getter;

@Getter
public class DataCloudJDBCException extends SQLException {
    /**
     * The fully formatted error message including customer detail and hint (while the normal exception message might not contain those based of the
     * `errorsIncludeCustomerDetails` property).
     */
    private String fullCustomerMessage;

    /**
     * The primary (terse) error message (without TraceId addition)
     */
    private String primaryMessage;

    /**
     * A suggestion on what what to do about the problem.
     * Differs from customer_detail by offering advise rather than hard facts.
     * Can be returned to the customer but in a cloud scenario where the query is coming from a third party likely shouldn't be logged.
     * Only makes sense to show to the user, if the user can actually change
     * the SQL query. Otherwise, this hint would probably not be actionable to
     * the user.
     */
    private String customerHint;

    /**
     * Error detail with data that is might be sensitive to the customer and thus shouldn't be logged in cloud services.
     */
    private String customerDetail;

    public DataCloudJDBCException(String reason) {
        super(reason);
    }

    public DataCloudJDBCException(String reason, String SQLState) {
        super(reason, SQLState);
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

    public DataCloudJDBCException(String reason, String fullCustomerMessage, String SQLState, Throwable cause) {
        super(reason, SQLState, cause);
        this.fullCustomerMessage = fullCustomerMessage;
    }

    public DataCloudJDBCException(
            String reason,
            String fullCustomerMessage,
            String SQLState,
            String primaryMessage,
            String customerHint,
            String customerDetail,
            Throwable cause) {
        super(reason, SQLState, cause);

        this.fullCustomerMessage = fullCustomerMessage;
        this.primaryMessage = primaryMessage;
        this.customerHint = customerHint;
        this.customerDetail = customerDetail;
    }
}
