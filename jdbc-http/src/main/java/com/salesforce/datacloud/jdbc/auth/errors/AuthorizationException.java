/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth.errors;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class AuthorizationException extends Exception {
    private final String message;
    private final String errorCode;
    private final String errorDescription;

    @Override
    public String getMessage() {
        return buildDetailedMessage();
    }

    private String buildDetailedMessage() {
        StringBuilder sb = new StringBuilder();
        if (message != null && !message.isEmpty()) {
            sb.append(message);
        } else {
            sb.append("Authorization failed");
        }

        if (errorCode != null && !errorCode.isEmpty()) {
            sb.append(" (HTTP ").append(errorCode).append(")");
        }

        if (errorDescription != null && !errorDescription.isEmpty()) {
            sb.append(": ").append(errorDescription);
        }

        return sb.toString();
    }

    /**
     * Determines if this exception represents a transient error that should be retried.
     *
     * Retriable errors: 5xx server errors and 429 Too Many Requests.
     * Non-retriable: 4xx client errors indicate permanent issues (invalid credentials, malformed request).
     *
     * @return true if this exception should be retried, false otherwise
     */
    public boolean isRetriable() {
        if (errorCode == null || errorCode.isEmpty()) {
            return false;
        }

        try {
            int httpCode = Integer.parseInt(errorCode);

            if (httpCode >= 500 && httpCode < 600) {
                return true;
            }

            if (httpCode == 429) {
                return true;
            }

            return false;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
