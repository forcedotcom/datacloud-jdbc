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
}
