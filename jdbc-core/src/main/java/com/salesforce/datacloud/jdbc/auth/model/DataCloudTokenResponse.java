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
