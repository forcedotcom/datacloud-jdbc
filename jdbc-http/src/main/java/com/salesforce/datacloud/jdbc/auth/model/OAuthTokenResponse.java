/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * The shape of this response can be found <a
 * href="https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm">here</a> under the
 * heading "Salesforce Grants a New Access Token"
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class OAuthTokenResponse implements AuthenticationResponseWithError {
    private String scope;

    @JsonProperty("access_token")
    private String token;

    @JsonProperty("instance_url")
    private String instanceUrl;

    @JsonProperty("token_type")
    private String tokenType;

    @JsonProperty("issued_at")
    private String issuedAt;

    @JsonProperty("error")
    private String errorCode;

    @JsonProperty("error_description")
    private String errorDescription;
}
