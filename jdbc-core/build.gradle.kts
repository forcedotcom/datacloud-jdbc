plugins {
    id("java-conventions")
    id("java-test-fixtures")
    id("publishing-conventions")
    alias(libs.plugins.lombok)
}

dependencies {
    compileOnly(project(":jdbc-grpc"))
    compileOnly(libs.grpc.stub)
    compileOnly(libs.grpc.protobuf)

    implementation(project(":jdbc-util"))
    implementation(libs.slf4j.api)
    implementation(libs.bundles.arrow)
    implementation(libs.apache.calcite.avatica)
    implementation(libs.guava)
    implementation(libs.jackson.databind)
    implementation(libs.failsafe)
    implementation(libs.apache.commons.lang3)

    testFixturesImplementation(project(":jdbc-grpc"))
    testFixturesImplementation(libs.slf4j.api)
    testFixturesImplementation(libs.guava)
    testFixturesImplementation(libs.jackson.databind)
    testFixturesImplementation(libs.grpc.stub)
    testFixturesImplementation(libs.grpc.protobuf)

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
