plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

dependencies {
    implementation("com.palantir.javaformat:gradle-palantir-java-format:2.61.0")
    implementation("dev.adamko.dev-publish:dev.adamko.dev-publish.gradle.plugin:0.4.2")
}
