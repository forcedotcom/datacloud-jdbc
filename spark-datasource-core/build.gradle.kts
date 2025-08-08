plugins {
    id("scala")
    id("scala-conventions")
    id("publishing-conventions")
}

description = "Salesforce Data Cloud Spark DataSource core implementation"
val mavenName: String by extra("Salesforce Data Cloud Spark DataSource Core")
val mavenDescription: String by extra("${project.description}")

dependencies {
    // ALIGNED WITH JDBC-CORE PATTERN: Use compileOnly for gRPC
    compileOnly(project(":jdbc-grpc"))
    compileOnly(libs.grpc.stub)
    compileOnly(libs.grpc.protobuf)
    
    // Core implementation dependencies
    implementation(project(":jdbc-core"))  // Need this for DataCloudConnection
    implementation(project(":jdbc-util"))
    implementation(libs.slf4j.api)
    
    // Spark dependencies
    implementation(libs.bundles.spark)

    // Test dependencies need the actual gRPC implementations
    testImplementation(project(":jdbc-grpc"))
    testImplementation(testFixtures(project(":jdbc-core")))
    testImplementation(libs.bundles.grpc.impl)
    testImplementation(libs.bundles.grpc.testing)
    testImplementation(libs.bundles.scala.testing)
}

tasks.named("compileScala") {
    dependsOn(":jdbc-grpc:compileJava")
    dependsOn(":jdbc-core:compileJava")
} 