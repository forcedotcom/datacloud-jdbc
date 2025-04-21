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




