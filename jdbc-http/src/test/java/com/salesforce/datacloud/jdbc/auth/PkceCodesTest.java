/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.security.SecureRandom;
import org.junit.jupiter.api.Test;

class PkceCodesTest {

    @Test
    void verifierAndChallengeAreBase64UrlNoPad() {
        PkceCodes codes = PkceCodes.generate();

        assertThat(codes.getVerifier())
                .as("verifier")
                .matches("^[A-Za-z0-9_-]+$")
                .hasSize(43);
        assertThat(codes.getChallenge())
                .as("challenge")
                .matches("^[A-Za-z0-9_-]+$")
                .hasSize(43);
    }

    @Test
    void challengeMatchesSha256OfVerifier() {
        PkceCodes codes = PkceCodes.generate();

        assertThat(codes.getChallenge())
                .as("challenge is base64url-no-pad sha256 of verifier")
                .isEqualTo(PkceCodes.s256Challenge(codes.getVerifier()));
    }

    @Test
    void s256ChallengeMatchesRfc7636AppendixBExample() {
        // RFC 7636 Appendix B test vector:
        //   verifier  "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        //   challenge "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        String verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        assertThat(PkceCodes.s256Challenge(verifier)).isEqualTo("E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
    }

    @Test
    void generateProducesDistinctVerifiersAcrossCalls() {
        // Same SecureRandom seed shouldn't matter — every call still draws fresh entropy.
        PkceCodes a = PkceCodes.generate(new SecureRandom());
        PkceCodes b = PkceCodes.generate(new SecureRandom());

        assertThat(a.getVerifier()).isNotEqualTo(b.getVerifier());
    }
}
