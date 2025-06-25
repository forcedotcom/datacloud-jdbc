plugins {
    `kotlin-dsl`
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

dependencies {
    implementation("com.google.osdetector:com.google.osdetector.gradle.plugin:1.7.3")
    implementation("de.undercouch.download:de.undercouch.download.gradle.plugin:5.6.0")
    implementation("dev.adamko.dev-publish:dev.adamko.dev-publish.gradle.plugin:0.4.2")
    implementation("com.diffplug.spotless:com.diffplug.spotless.gradle.plugin:7.0.2")
}
