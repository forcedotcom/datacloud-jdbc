plugins {
    id("java-conventions")
    id("publishing-conventions")
    alias(libs.plugins.lombok)
}

dependencies {
    implementation(project(":jdbc-util"))
    implementation(libs.okhttp3.client)
    implementation(libs.okhttp3.logging.interceptor)
    implementation(libs.slf4j.api)

    implementation(libs.guava)

    implementation(libs.jackson.databind)

    implementation(libs.failsafe)

    implementation(libs.apache.commons.lang3)

    implementation(libs.jjwt.api)

    runtimeOnly(libs.jjwt.impl)

    runtimeOnly(libs.jjwt.jackson)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.bundles.mocking)
}
