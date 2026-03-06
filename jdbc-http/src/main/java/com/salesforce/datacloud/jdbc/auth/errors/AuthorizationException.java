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
     * Retries are appropriate for:
     * - 5xx server errors (500, 502, 503, 504)
     * - 400 errors with retry hints in the error description (e.g., "retry your request", "unknown_error")
     *
     * @return true if this exception should be retried, false otherwise
     */
    public boolean isRetriable() {
        if (errorCode == null || errorCode.isEmpty()) {
            return false;
        }

        try {
            int httpCode = Integer.parseInt(errorCode);

            // Retry on 5xx server errors (transient server issues)
            if (httpCode >= 500 && httpCode < 600) {
                return true;
            }

            // Retry on 429 (Too Many Requests) - always retriable
            if (httpCode == 429) {
                return true;
            }

            // Retry on 400 errors that indicate transient issues
            if (httpCode == 400) {
                String description = errorDescription != null ? errorDescription.toLowerCase() : "";
                // Check for retry hints in the error description
                return description.contains("retry")
                        || description.contains("unknown_error")
                        || description.contains("temporary")
                        || description.contains("rate limit")
                        || description.contains("throttle");
            }

            return false;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
