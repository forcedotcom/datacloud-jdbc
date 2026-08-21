plugins {
    id("java-conventions")
    id("publishing-conventions")
    alias(libs.plugins.lombok)
}

description = "HTTP utilities including Auth implementations for establishing a connection and SOQL for fetching metadata for Salesforce Data Cloud JDBC driver"
extra.set("mavenName", "Salesforce Data Cloud JDBC HTTP")
extra.set("mavenDescription", project.description.toString())

dependencies {
    implementation(project(":jdbc-util"))
    implementation(libs.okhttp3.client)
    implementation(libs.okhttp3.logging.interceptor)
    implementation(libs.slf4j.api)

    implementation(libs.guava)

    implementation(libs.jackson.databind)

    implementation(libs.failsafe)

    implementation(libs.apache.commons.lang3)

    implementation(libs.jjwt.api)

    runtimeOnly(libs.jjwt.impl)

    runtimeOnly(libs.jjwt.jackson)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.bundles.mocking)
}
