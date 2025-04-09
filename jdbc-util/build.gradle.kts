plugins {
    id("java-conventions")
    id("publishing-conventions")
    alias(libs.plugins.lombok)
}

dependencies {
    implementation(libs.slf4j.api)

    implementation(libs.guava)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.bundles.mocking)
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
    dependsOn("generateVersionProperties")
}
