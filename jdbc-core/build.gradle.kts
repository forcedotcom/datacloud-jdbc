plugins {
    id("java-conventions")
    alias(libs.plugins.lombok)
}

description = "Salesforce Data Cloud JDBC Core"

dependencies {
    api(libs.slf4j.api)
    api(project(":jdbc-grpc"))

    implementation(libs.apache.calcite.avatica)
    implementation(libs.guava)

    implementation(libs.jackson.databind)

    implementation(libs.okhttp3)

    implementation(libs.failsafe)

    implementation(libs.apache.commons.lang3)

    implementation(libs.apache.arrow.vector)
    runtimeOnly(libs.apache.arrow.memory.netty)


    implementation(libs.jjwt.api)
    runtimeOnly(libs.jjwt.impl)
    runtimeOnly(libs.jjwt.jackson)

    testImplementation(project(":jdbc-grpc"))

    testImplementation(libs.grpc.netty)
    testImplementation(libs.grpc.protobuf)
    testImplementation(libs.grpc.stub)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.bundles.mocking)
    testImplementation(libs.bundles.grpc)
}

tasks.register("generateVersionProperties") {
    val resourcesDir = layout.buildDirectory.dir("resources/main")
    val version = project.version
    outputs.dir(resourcesDir)

    doLast {
        val propertiesFile = resourcesDir.get().file("driver-version.properties")
        propertiesFile.asFile.parentFile.mkdirs()
        propertiesFile.asFile.writeText("version=$version")
    }
}

tasks.named("compileJava") {
    dependsOn("generateVersionProperties", ":jdbc-grpc:build")
}
