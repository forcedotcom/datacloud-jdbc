plugins {
    id("java-conventions")
    alias(libs.plugins.lombok)
}

description = "Integration test module for shaded JDBC driver"

dependencies {
    // Test framework
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    
    // Logging
    testImplementation(libs.slf4j.simple)
}

// Configure test task to NOT run integration tests by default
tasks.test {
    group = "verification"
    description = "Runs unit tests (integration tests are run separately with runIntegrationTest)"
    
    // Exclude integration-tagged tests from regular test execution
    useJUnitPlatform {
        excludeTags("integration")
    }
    
    // Exit with non-zero code on failure
    ignoreFailures = false
}

// Integration test task - runs ONLY integration tests with proper configuration
tasks.register("runIntegrationTest", Test::class) {
    group = "verification"
    description = "Runs integration tests against the shaded JDBC JAR (requires DATACLOUD credentials)"
    
    // Copy configuration from main test task
    dependsOn(":jdbc:shadowJar")
    classpath += files(
        project(":jdbc").tasks.named("shadowJar").map { it.outputs.files.singleFile }
    )
    
    // Pass system properties for test configuration
    systemProperty("test.connection.url", System.getProperty("test.connection.url", ""))
    systemProperty("test.connection.userName", System.getProperty("test.connection.userName", ""))
    systemProperty("test.connection.password", System.getProperty("test.connection.password", ""))
    systemProperty("test.connection.clientId", System.getProperty("test.connection.clientId", ""))
    systemProperty("test.connection.clientSecret", System.getProperty("test.connection.clientSecret", ""))
    
    
    // Only run integration-tagged tests
    useJUnitPlatform {
        includeTags("integration")
    }
    
    ignoreFailures = false
}

// Note: Integration test is run explicitly in CI/CD workflows, not as part of regular build
