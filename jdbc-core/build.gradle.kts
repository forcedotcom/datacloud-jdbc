plugins {
    id("java-conventions")
    id("publishing-conventions")
    alias(libs.plugins.lombok)
}

dependencies {
    constraints {
        implementation(libs.netty.common) {
            because("https://www.mend.io/vulnerability-database/CVE-2024-47535")
        }

        implementation(libs.netty.handler) {
            because("https://www.mend.io/vulnerability-database/CVE-2025-24970")
        }
    }

    api(project(":jdbc-grpc"))

    implementation(libs.slf4j.api)

    implementation(libs.bundles.grpc)

    implementation(libs.bundles.arrow)

    implementation(libs.apache.calcite.avatica)

    implementation(libs.guava)

    implementation(libs.jackson.databind)

    implementation(libs.okhttp3)

    implementation(libs.failsafe)

    implementation(libs.apache.commons.lang3)

    implementation(libs.jjwt.api)

    runtimeOnly(libs.jjwt.impl)

    runtimeOnly(libs.jjwt.jackson)

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
        logger.lifecycle("version written to driver-version.properties. version=$version")
    }
}

tasks.named("compileJava") {
    dependsOn("generateVersionProperties", ":jdbc-grpc:compileJava")
}
