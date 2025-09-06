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
 * href="https://developer.salesforce.com/docs/atlas.en-us.c360a_api.meta/c360a_api/c360a_getting_started_with_cdp.htm">here</a>
 * under the heading "Exchanging Access Token for Data Cloud Token"
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataCloudTokenResponse implements AuthenticationResponseWithError {
    @JsonProperty("access_token")
    private String token;

    @JsonProperty("instance_url")
    private String instanceUrl;

    @JsonProperty("token_type")
    private String tokenType;

    @JsonProperty("expires_in")
    private int expiresIn;

    @JsonProperty("error")
    private String errorCode;

    @JsonProperty("error_description")
    private String errorDescription;
}
