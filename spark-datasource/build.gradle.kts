plugins {
    id("scala")
    id("java-conventions")
    id("publishing-conventions")
}

dependencies {
    implementation(project(":jdbc"))
    implementation(project(":jdbc-grpc"))
    implementation(project(":jdbc-core"))
    implementation(project(":jdbc-util"))
    implementation(libs.bundles.grpc.impl)
    implementation("org.apache.spark:spark-sql_2.13:3.5.5")
    implementation("org.apache.spark:spark-core_2.13:3.5.5")

    testImplementation(testFixtures(project(":jdbc-core")))
    testImplementation("org.scalatest:scalatest_3:3.2.19")
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