/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DataCloudIntervalTest {

    /**
     * Locales whose default digits are not ASCII (Arabic-Indic and Devanagari). {@code String.format}
     * without an explicit locale would render the fractional seconds in those digits.
     */
    @ParameterizedTest
    @ValueSource(strings = {"ar-EG", "hi-IN-u-nu-deva"})
    void toStringUsesAsciiDigitsRegardlessOfDefaultLocale(String languageTag) {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag(languageTag));

            assertThat(new DataCloudInterval(0, 0, 1_500_000_000L)).hasToString("PT1.500000S");
            assertThat(new DataCloudInterval(0, 0, -1_500_000_000L)).hasToString("PT-1.500000S");
            assertThat(new DataCloudInterval(0, 0, 1L)).hasToString("PT0.000000001S");
            assertThat(new DataCloudInterval(14, 3, 14_706_789_000_000L)).hasToString("P1Y2M3DT4H5M6.789000S");
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        DataCloudInterval interval = new DataCloudInterval(-14, 3, -5_405_000_000_000L);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(interval);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            assertThat(in.readObject()).isEqualTo(interval).hasToString(interval.toString());
        }
    }

    @Test
    void equalsAndHashCodeHonourTheContract() {
        DataCloudInterval interval = new DataCloudInterval(1, 2, 3);

        assertThat(interval).isEqualTo(interval);
        assertThat(interval).isEqualTo(new DataCloudInterval(1, 2, 3));
        assertThat(interval.hashCode()).isEqualTo(new DataCloudInterval(1, 2, 3).hashCode());
        assertThat(interval).isNotEqualTo(null);
        assertThat(interval).isNotEqualTo("P1M2DT0.000000003S");
        assertThat(interval).isNotEqualTo(new DataCloudInterval(0, 2, 3));
        assertThat(interval).isNotEqualTo(new DataCloudInterval(1, 0, 3));
        assertThat(interval).isNotEqualTo(new DataCloudInterval(1, 2, 0));
    }
}
