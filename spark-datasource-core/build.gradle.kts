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
    implementation("org.apache.spark:spark-sql_2.13:3.5.5")
    implementation("org.apache.spark:spark-core_2.13:3.5.5")

    // Test dependencies need the actual gRPC implementations
    testImplementation(project(":jdbc-grpc"))
    testImplementation(testFixtures(project(":jdbc-core")))
    testImplementation("org.scalatest:scalatest_3:3.2.19")
    testImplementation(libs.bundles.grpc.impl)
    testImplementation(libs.bundles.grpc.testing)
    testRuntimeOnly("org.junit.platform:junit-platform-engine:1.12.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.12.0")
    testRuntimeOnly("org.scalatestplus:junit-5-12_3:3.2.19.0")
}

tasks {
    test{
        useJUnitPlatform {
            includeEngines("scalatest")
            testLogging {
                events("passed", "skipped", "failed")
            }
        }
    }
}

tasks.named("compileScala") {
    dependsOn(":jdbc-grpc:compileJava")
    dependsOn(":jdbc-core:compileJava")
} 