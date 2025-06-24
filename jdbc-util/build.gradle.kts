plugins {
    id("java-conventions")
    id("publishing-conventions")
    alias(libs.plugins.lombok)
}

description = "Utilities for Java's Stream, Properties, String, etc. for Salesforce Data Cloud JDBC driver"
val mavenName: String by extra("Salesforce Data Cloud JDBC Utilities")
val mavenDescription: String by extra("${project.description}")

dependencies {
    implementation(libs.slf4j.api)

    implementation(libs.guava)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.bundles.mocking)
}




