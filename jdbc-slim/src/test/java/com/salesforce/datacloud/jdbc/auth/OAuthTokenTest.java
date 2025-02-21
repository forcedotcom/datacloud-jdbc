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
package com.salesforce.datacloud.jdbc.auth;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.auth.model.OAuthTokenResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Messages;
import lombok.val;
import org.junit.jupiter.api.Test;

class OAuthTokenTest {
    @Test
    void throwsOnBadInstanceUrl() {
        val response = new OAuthTokenResponse();
        response.setToken("not empty");
        response.setInstanceUrl("%&#(");
        val ex = assertThrows(DataCloudJDBCException.class, () -> OAuthToken.of(response));
        assertThat(ex).hasMessage(Messages.FAILED_LOGIN);
    }

    @Test
    void throwsOnBadToken() {
        val response = new OAuthTokenResponse();
        response.setInstanceUrl("login.salesforce.com");
        val ex = assertThrows(DataCloudJDBCException.class, () -> OAuthToken.of(response));
        assertThat(ex).hasMessage(Messages.FAILED_LOGIN).hasNoCause();
    }
}
