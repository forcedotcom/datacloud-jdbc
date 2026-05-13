plugins {
    id("java-conventions")
    id("java-test-fixtures")
    id("publishing-conventions")
    alias(libs.plugins.lombok)
}

description = "Salesforce Data Cloud JDBC core implementation"
val mavenName: String by extra("Salesforce Data Cloud JDBC Core")
val mavenDescription: String by extra("${project.description}")

dependencies {
    compileOnly(project(":jdbc-grpc"))
    compileOnly(libs.grpc.stub)
    compileOnly(libs.grpc.protobuf)
    compileOnly(libs.grpc.netty)  // For DirectDataCloudConnection SSL support

    implementation(project(":jdbc-util"))
    implementation(libs.slf4j.api)
    implementation(libs.bundles.arrow)
    implementation(libs.guava)
    implementation(libs.jackson.databind)
    implementation(libs.failsafe)
    implementation(libs.apache.commons.lang3)

    testFixturesImplementation(project(":jdbc-grpc"))
    testFixturesImplementation(platform(libs.junit.bom))
    testFixturesImplementation(libs.slf4j.api)
    testFixturesImplementation(libs.guava)
    testFixturesImplementation(libs.jackson.databind)
    testFixturesImplementation(libs.grpc.stub)
    testFixturesImplementation(libs.grpc.protobuf)
    testFixturesImplementation(libs.junit.platform.launcher)

    testImplementation(project(":jdbc-grpc"))
    testImplementation(project(":jdbc-reference"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.bundles.mocking)
    testImplementation(libs.bundles.grpc.impl)
    testImplementation(libs.bundles.grpc.testing)
}

tasks.named("compileJava") {
    dependsOn(":jdbc-grpc:compileJava")
}

// Iceberg sets -Darrow.enable_null_check_for_get=false on the JVM. With that flag off, Arrow's
// VarCharVector/VarBinaryVector/FixedSizeBinaryVector .get(int) skip the validity check and return
// stale buffer bytes for null rows instead of null. Run the test suite under that condition so
// the existing null-handling assertions cover the Iceberg scenario.
tasks.test {
    systemProperty("arrow.enable_null_check_for_get", "false")
}
