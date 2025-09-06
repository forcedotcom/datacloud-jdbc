/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth.model;

/**
 * * Check out the error code <a
 * href="https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_flow_errors.htm">docs</a>
 */
public interface AuthenticationResponseWithError {
    String getToken();

    String getErrorCode();

    String getErrorDescription();
}
