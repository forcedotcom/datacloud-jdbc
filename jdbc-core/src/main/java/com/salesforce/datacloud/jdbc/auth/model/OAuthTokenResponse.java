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
package com.salesforce.datacloud.jdbc.auth.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * The shape of this response can be found <a
 * href="https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5">here</a> under the
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
