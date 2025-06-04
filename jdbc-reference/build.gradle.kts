plugins {
    id("java-conventions")
    alias(libs.plugins.lombok)
    application
}

dependencies {
    implementation(libs.slf4j.api)

    implementation(libs.guava)
    implementation(libs.pgjdbc)
    implementation(libs.jackson.databind)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.bundles.mocking)
}

application {
    mainClass = "com.salesforce.datacloud.reference.PostgresReferenceGenerator"
}
