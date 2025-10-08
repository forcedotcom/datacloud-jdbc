/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.auth.model.OAuthTokenResponse;
import java.sql.SQLException;
import lombok.val;
import org.junit.jupiter.api.Test;

class OAuthTokenTest {
    @Test
    void throwsOnBadInstanceUrl() {
        val response = new OAuthTokenResponse();
        response.setToken("not empty");
        response.setInstanceUrl("%&#(");
        val ex = assertThrows(SQLException.class, () -> OAuthToken.of(response));
        assertThat(ex).hasMessage(OAuthToken.FAILED_LOGIN);
    }

    @Test
    void throwsOnBadToken() {
        val response = new OAuthTokenResponse();
        response.setInstanceUrl("login.salesforce.com");
        val ex = assertThrows(SQLException.class, () -> OAuthToken.of(response));
        assertThat(ex).hasMessage(OAuthToken.FAILED_LOGIN).hasNoCause();
    }
}
