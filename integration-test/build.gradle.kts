plugins {
    application
    id("java-conventions")
    alias(libs.plugins.lombok)
}

description = "Integration test application for shaded JDBC driver"

dependencies {
    // Test framework
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    
    // Logging
    implementation(libs.slf4j.simple)
}

application {
    mainClass.set("com.salesforce.datacloud.jdbc.integration.ShadedJarIntegrationTest")
}

// Task to run integration tests against the shaded JAR
tasks.register<JavaExec>("runIntegrationTest") {
    dependsOn(":jdbc:shadowJar")
    group = "verification"
    description = "Runs integration test against the shaded JDBC JAR"
    
    // Build classpath with the shaded JAR and test dependencies
    // Use lazy evaluation to resolve the JAR path at execution time
    classpath = sourceSets.main.get().runtimeClasspath + files(
        provider {
            project(":jdbc").layout.buildDirectory.file("libs").get().asFile
                .listFiles { _, name -> name.endsWith("-shaded.jar") }?.firstOrNull()
                ?: throw GradleException("Shaded JAR not found in ${project(":jdbc").layout.buildDirectory.file("libs").get()}")
        }
    )
    
    mainClass.set("com.salesforce.datacloud.jdbc.integration.ShadedJarIntegrationTest")
    
    // Required JVM arguments for Apache Arrow (shaded) - only needed for Java 9+
    // Java 8 doesn't have the module system, so these aren't required
    val javaVersion = JavaVersion.current()
    if (javaVersion.isJava9Compatible) {
        jvmArgs(
            "--add-opens=java.base/java.nio=com.salesforce.datacloud.shaded.org.apache.arrow.memory.core,ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=com.salesforce.datacloud.shaded.org.apache.arrow.memory.core,ALL-UNNAMED",
            "--add-opens=java.base/java.lang=com.salesforce.datacloud.shaded.org.apache.arrow.memory.core,ALL-UNNAMED"
        )
    }
    
    // Pass system properties for test configuration
    systemProperty("test.connection.url", System.getProperty("test.connection.url", ""))
    systemProperty("test.connection.userName", System.getProperty("test.connection.userName", ""))
    systemProperty("test.connection.password", System.getProperty("test.connection.password", ""))
    systemProperty("test.connection.clientId", System.getProperty("test.connection.clientId", ""))
    systemProperty("test.connection.clientSecret", System.getProperty("test.connection.clientSecret", ""))
    
    // Exit with non-zero code on failure
    isIgnoreExitValue = false
}

// Note: Integration test is run explicitly in CI/CD workflows, not as part of regular build
