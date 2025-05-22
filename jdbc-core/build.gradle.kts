plugins {
    id("java-conventions")
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

    implementation(libs.failsafe)

    implementation(libs.apache.commons.lang3)

    testImplementation(project(":jdbc-grpc"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.bundles.mocking)
    testImplementation(libs.bundles.grpc.impl)
    testImplementation(libs.bundles.grpc.testing)
}

tasks.named("compileJava") {
    dependsOn(":jdbc-grpc:compileJava")
}
