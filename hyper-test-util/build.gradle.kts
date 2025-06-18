plugins {
    id("java-conventions")
    alias(libs.plugins.lombok)
}

dependencies {
    implementation(platform(libs.junit.bom))
    implementation(libs.bundles.testing)
    implementation(libs.jackson.databind)
    implementation(libs.guava)
}
