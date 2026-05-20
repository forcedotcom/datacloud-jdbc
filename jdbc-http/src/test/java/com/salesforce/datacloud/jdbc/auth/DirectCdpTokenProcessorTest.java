/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DirectCdpTokenProcessorTest {

    private static final String TENANT_HOST = "test.c360a.salesforce.com";
    private static final String TENANT_ID = "a360/falcondev/a6d726a73f534327a6a8e2e0f3cc3840";

    /**
     * Builds an unsigned JWT with the given audienceTenantId and exp claim. The {@link DataCloudToken}
     * decoder only base64-parses the payload — it does not verify the signature — so a fixed bogus
     * signature is enough to exercise the production path.
     */
    private static String jwtWithExp(long expEpochSeconds) {
        try {
            ObjectNode header = new ObjectMapper().createObjectNode();
            header.put("alg", "ES256");
            header.put("typ", "JWT");

            ObjectNode payload = new ObjectMapper().createObjectNode();
            payload.put("audienceTenantId", TENANT_ID);
            payload.put("exp", expEpochSeconds);

            Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
            String h = enc.encodeToString(header.toString().getBytes(StandardCharsets.UTF_8));
            String p = enc.encodeToString(payload.toString().getBytes(StandardCharsets.UTF_8));
            return h + "." + p + ".sig";
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String validJwt() {
        return jwtWithExp(Instant.now().getEpochSecond() + 3600);
    }

    private static Properties propertiesForCdpToken(String cdpToken, String tenantUrl) {
        Properties properties = new Properties();
        properties.setProperty("cdpToken", cdpToken);
        properties.setProperty("tenantUrl", tenantUrl);
        return properties;
    }

    @Test
    void getDataCloudTokenReturnsValidToken() throws SQLException {
        String token = validJwt();
        DirectCdpTokenProcessor processor =
                DirectCdpTokenProcessor.ofDestructive(propertiesForCdpToken(token, TENANT_HOST));
        DataCloudToken dcToken = processor.getDataCloudToken();

        assertThat(dcToken.getAccessToken()).isEqualTo("Bearer " + token);
        assertThat(dcToken.getTenantUrl()).isEqualTo(TENANT_HOST);
        assertThat(dcToken.getTenantId()).isEqualTo(TENANT_ID);
        assertThat(dcToken.isAlive()).isTrue();
    }

    @Test
    void ofDestructiveRejectsTenantUrlWithScheme() {
        for (String invalid : new String[] {
            "https://" + TENANT_HOST, "http://" + TENANT_HOST, TENANT_HOST + ":443", TENANT_HOST + "/",
        }) {
            Properties props = propertiesForCdpToken(validJwt(), invalid);
            SQLException ex = assertThrows(
                    SQLException.class,
                    () -> DirectCdpTokenProcessor.ofDestructive(props),
                    "Expected rejection of: " + invalid);
            assertThat(ex.getMessage()).contains("bare hostname");
        }
    }

    @Test
    void ofDestructiveRejectsTenantUrlWithWhitespace() {
        Properties props = propertiesForCdpToken(validJwt(), "  " + TENANT_HOST + "  ");
        SQLException ex = assertThrows(SQLException.class, () -> DirectCdpTokenProcessor.ofDestructive(props));
        assertThat(ex.getMessage()).contains("whitespace");
    }

    @Test
    void getDataCloudTokenReturnsCachedToken() throws SQLException {
        DirectCdpTokenProcessor processor =
                DirectCdpTokenProcessor.ofDestructive(propertiesForCdpToken(validJwt(), TENANT_HOST));
        DataCloudToken first = processor.getDataCloudToken();
        DataCloudToken second = processor.getDataCloudToken();

        assertThat(first).isSameAs(second);
    }

    @Test
    void getLakehouseNameWithoutDataspace() throws SQLException {
        DirectCdpTokenProcessor processor =
                DirectCdpTokenProcessor.ofDestructive(propertiesForCdpToken(validJwt(), TENANT_HOST));
        assertThat(processor.getLakehouseName()).isEqualTo("lakehouse:" + TENANT_ID + ";");
    }

    @Test
    void getLakehouseNameWithDataspace() throws SQLException {
        String dataspace = UUID.randomUUID().toString();
        Properties props = propertiesForCdpToken(validJwt(), TENANT_HOST);
        props.setProperty("dataspace", dataspace);

        DirectCdpTokenProcessor processor = DirectCdpTokenProcessor.ofDestructive(props);
        assertThat(processor.getLakehouseName()).isEqualTo("lakehouse:" + TENANT_ID + ";" + dataspace);
    }

    @Test
    void ofDestructiveThrowsWhenCdpTokenAlreadyExpired() {
        Properties props = propertiesForCdpToken(jwtWithExp(Instant.now().getEpochSecond() - 1), TENANT_HOST);
        SQLException ex = assertThrows(SQLException.class, () -> DirectCdpTokenProcessor.ofDestructive(props));
        assertThat(ex.getMessage()).contains("expired");
    }

    @Test
    void ofDestructiveThrowsWhenCdpTokenMissing() {
        Properties props = new Properties();
        props.setProperty("tenantUrl", TENANT_HOST);

        SQLException ex = assertThrows(SQLException.class, () -> DirectCdpTokenProcessor.ofDestructive(props));
        assertThat(ex.getMessage()).contains("cdpToken");
    }

    @Test
    void ofDestructiveThrowsWhenTenantUrlMissing() {
        Properties props = new Properties();
        props.setProperty("cdpToken", validJwt());

        SQLException ex = assertThrows(SQLException.class, () -> DirectCdpTokenProcessor.ofDestructive(props));
        assertThat(ex.getMessage()).contains("tenantUrl");
    }

    @Test
    void ofDestructiveThrowsWhenCdpTokenIsInvalidJwt() {
        Properties props = propertiesForCdpToken("not-a-valid-jwt", TENANT_HOST);

        SQLException ex = assertThrows(SQLException.class, () -> DirectCdpTokenProcessor.ofDestructive(props));
        assertThat(ex.getMessage()).contains("not a valid JWT");
    }

    @Test
    void ofDestructiveRemovesPropertiesFromInput() throws SQLException {
        Properties props = propertiesForCdpToken(validJwt(), TENANT_HOST);
        props.setProperty("dataspace", "myspace");

        DirectCdpTokenProcessor.ofDestructive(props);

        assertThat(props.containsKey("cdpToken")).isFalse();
        assertThat(props.containsKey("tenantUrl")).isFalse();
        assertThat(props.containsKey("dataspace")).isFalse();
    }

    @Test
    void hasCdpTokenReturnsTrueWhenBothPresent() {
        Properties props = propertiesForCdpToken(validJwt(), TENANT_HOST);
        assertThat(DirectCdpTokenProcessor.hasCdpToken(props)).isTrue();
    }

    @Test
    void hasCdpTokenReturnsFalseWhenMissing() {
        assertThat(DirectCdpTokenProcessor.hasCdpToken(new Properties())).isFalse();
        assertThat(DirectCdpTokenProcessor.hasCdpToken(null)).isFalse();

        Properties onlyCdpToken = new Properties();
        onlyCdpToken.setProperty("cdpToken", validJwt());
        assertThat(DirectCdpTokenProcessor.hasCdpToken(onlyCdpToken)).isFalse();

        Properties onlyTenantUrl = new Properties();
        onlyTenantUrl.setProperty("tenantUrl", TENANT_HOST);
        assertThat(DirectCdpTokenProcessor.hasCdpToken(onlyTenantUrl)).isFalse();
    }

    @Test
    void expFromJwtReturnsExpFromValidJwt() throws SQLException {
        long futureExp = Instant.now().getEpochSecond() + 1234;
        assertThat(DirectCdpTokenProcessor.expFromJwt(jwtWithExp(futureExp))).isEqualTo(futureExp);
    }

    @Test
    void expFromJwtThrowsWhenJwtSingleSegment() {
        SQLException ex =
                assertThrows(SQLException.class, () -> DirectCdpTokenProcessor.expFromJwt("only-one-segment"));
        assertThat(ex.getMessage()).contains("at least 2 segments");
    }

    @Test
    void expFromJwtThrowsWhenPayloadNotBase64() {
        // Two segments but second one is not valid base64url
        SQLException ex =
                assertThrows(SQLException.class, () -> DirectCdpTokenProcessor.expFromJwt("header.@@not-base64@@.sig"));
        assertThat(ex.getMessage()).contains("Failed to parse CDP token JWT");
    }

    @Test
    void expFromJwtThrowsWhenExpClaimMissing() {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = enc.encodeToString("{\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String payload = enc.encodeToString("{\"sub\":\"x\"}".getBytes(StandardCharsets.UTF_8));
        String jwt = header + "." + payload + ".sig";
        SQLException ex = assertThrows(SQLException.class, () -> DirectCdpTokenProcessor.expFromJwt(jwt));
        assertThat(ex.getMessage()).contains("missing a numeric 'exp' claim");
    }
}
