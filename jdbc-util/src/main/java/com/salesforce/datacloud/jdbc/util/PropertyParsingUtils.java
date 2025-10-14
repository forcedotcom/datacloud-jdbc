/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import com.google.common.collect.ImmutableSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility methods for parsing our settings out of the Properties object.
 *
 * We remove the parsed properties from the Properties object. After parsing
 * finished, we check if any properties are left in the Properties object. If so,
 * we throw an exception in `validateRemainingProperties`.
 *
 * Be careful when multiple pieces accept the same property, in particular using
 * `takeOptional`. The first call to `takeOptional` will remove the property from
 * the Properties object, so the second call will not find it anymore.
 */
public final class PropertyParsingUtils {
    private PropertyParsingUtils() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static Optional<String> takeOptional(Properties properties, String key) {
        if (properties == null) {
            return Optional.empty();
        }

        Object value = properties.remove(key);
        if (value == null) {
            return Optional.empty();
        }
        if (!(value instanceof String)) {
            throw new IllegalArgumentException("Property `" + key + "` is a non-string value");
        }
        String strValue = (String) value;
        return Optional.of(strValue);
    }

    public static String takeRequired(Properties properties, String key) {
        Optional<String> result = takeOptional(properties, key);
        if (!result.isPresent()) {
            throw new IllegalArgumentException("Property `" + key + "` is missing");
        }
        return result.get();
    }

    public static Optional<Boolean> takeOptionalBoolean(Properties properties, String key) {
        return takeOptional(properties, key).map(str -> {
            if (str.equals("true")) {
                return true;
            } else if (str.equals("false")) {
                return false;
            } else {
                throw new IllegalArgumentException("Failed to parse `" + key + "` property as a boolean");
            }
        });
    }

    public static Optional<Integer> takeOptionalInteger(Properties properties, String key) {
        return takeOptional(properties, key).map(str -> {
            try {
                return Integer.parseInt(str);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Failed to parse `" + key + "` property as an integer: " + e.getMessage());
            }
        });
    }

    public static Optional<Duration> takeOptionalDuration(Properties properties, String key) {
        return takeOptionalInteger(properties, key).map(str -> {
            Duration duration = Duration.ofSeconds(str);
            if (duration.isNegative()) {
                throw new IllegalArgumentException("Property `" + key + "` must be a positive number of seconds");
            }
            return duration;
        });
    }

    public static <T extends Enum<T>> Optional<T> takeOptionalEnum(
            Properties properties, String key, Class<T> enumClass) {
        return takeOptional(properties, key).map(str -> {
            try {
                return Enum.valueOf(enumClass, str);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Failed to parse `" + key + "` property. Valid values are: "
                        + Arrays.toString(enumClass.getEnumConstants()));
            }
        });
    }

    public static Optional<List<String>> takeOptionalList(Properties properties, String key) {
        return takeOptional(properties, key)
                .map(str -> Arrays.stream(str.split(",")).collect(Collectors.toList()));
    }

    public static void validateRemainingProperties(Properties properties) throws SQLException {
        if (properties == null) {
            return;
        }

        // Detect a couple of common Hyper session settings that users sometimes try to pass without
        // the required "querySetting." prefix. We proactively provide an actionable error message.
        final Set<String> COMMON_HYPER_SETTINGS = ImmutableSet.of("time_zone", "lc_time");
        for (String rawKey : properties.stringPropertyNames()) {
            String canonical = rawKey.trim().toLowerCase().replace('-', '_');
            if (COMMON_HYPER_SETTINGS.contains(canonical)) {
                throw new SQLException("Invalid property '" + rawKey + "'. Use 'querySetting." + canonical
                        + "' to set the query setting.");
            }
        }

        // If there are any remaining properties, throw an exception.
        if (!properties.isEmpty()) {
            throw new SQLException("Unknown JDBC properties: " + String.join(", ", properties.stringPropertyNames()));
        }
    }
}
