plugins {
    id("java-conventions")
    alias(libs.plugins.lombok)
}

description = "Salesforce Data Cloud JDBC Core"

dependencies {
    api(project(":jdbc-grpc"))

    api(libs.com.fasterxml.jackson.core.jackson.databind)
    api(libs.com.google.guava)
    api(libs.com.squareup.okhttp3.okhttp)
    api(libs.io.jsonwebtoken.jjwt.api)
    api(libs.javax.annotation.javax.annotation.api)
    api(libs.failsafe)
    api(libs.apache.arrow.vector)
    api(libs.apache.calcite.avatica.avatica)
    api(libs.apache.commons.commons.lang3)
    api(libs.slf4j.api)

    runtimeOnly(libs.io.jsonwebtoken.jjwt.impl)
    runtimeOnly(libs.io.jsonwebtoken.jjwt.jackson)
    runtimeOnly(libs.apache.arrow.memory.netty)

    testImplementation(project(":jdbc-grpc"))

    testImplementation(libs.grpc.netty)
    testImplementation(libs.grpc.protobuf)
    testImplementation(libs.grpc.stub)
    testImplementation(libs.javax.annotation.javax.annotation.api)

    testImplementation(libs.com.fasterxml.jackson.core.jackson.databind)
    testImplementation(libs.com.google.guava)
    testImplementation(libs.com.squareup.okhttp3.okhttp)
    testImplementation(libs.io.jsonwebtoken.jjwt.api)
    testImplementation(libs.javax.annotation.javax.annotation.api)
    testImplementation(libs.failsafe)
    testImplementation(libs.apache.arrow.vector)
    testImplementation(libs.apache.calcite.avatica.avatica)
    testImplementation(libs.apache.commons.commons.lang3)

    testImplementation(libs.com.fasterxml.jackson.core.jackson.databind)
    testImplementation(libs.com.google.guava)
    testImplementation(libs.com.squareup.okhttp3.okhttp)
    testImplementation(libs.io.jsonwebtoken.jjwt.api)
    testImplementation(libs.javax.annotation.javax.annotation.api)
    testImplementation(libs.failsafe)
    testImplementation(libs.apache.arrow.vector)
    testImplementation(libs.apache.calcite.avatica.avatica)
    testImplementation(libs.apache.commons.commons.lang3)
    testImplementation(libs.slf4j.api)


    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.bundles.mocking)

    testImplementation(libs.apache.arrow.memory.netty)
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
    dependsOn("generateVersionProperties")
}
