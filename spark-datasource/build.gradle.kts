plugins {
    id("scala")
    id("scala-conventions")
    id("publishing-conventions")
}

description = "Spark Datasource for Salesforce Data Cloud JDBC"

val mavenName: String by extra("Spark Datasource for Salesforce Data Cloud JDBC")
val mavenDescription: String by extra("${project.description}")

dependencies {
    implementation(project(":jdbc"))
    implementation(project(":jdbc-grpc"))
    implementation(project(":jdbc-core"))
    implementation(project(":jdbc-util"))
    implementation(libs.bundles.grpc.impl)
    implementation(libs.bundles.spark)

    // Override transitive Jackson Scala module from Spark with newer version
    implementation(libs.jackson.module.scala)

    testImplementation(platform(libs.junit.bom))
    testImplementation(testFixtures(project(":jdbc-core")))
    testImplementation(libs.scalatest.base)
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.scalatest.junit)
}

tasks {
    test {
        useJUnitPlatform {
            includeEngines("scalatest")
            testLogging { events("passed", "skipped", "failed") }
        }
    }
}
