plugins {
    id("base-conventions")
    `java-library`
    jacoco
}

group = "com.salesforce.datacloud"

repositories {
    mavenLocal()
    maven { url = uri("https://repo.maven.apache.org/maven2/") }
}

tasks.withType<Test>().configureEach {
    javaLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(8) }

    useJUnitPlatform()

    testLogging {
        events("passed", "skipped", "failed")
        showExceptions = true
        showStandardStreams = true
        showStackTraces = true
    }

    jvmArgs("-Xmx1g", "-Xms512m")

    jacoco { exclude("salesforce/**") }
}

fun JacocoReportBase.excludeGrpc() {
    classDirectories.setFrom(
        files(
            classDirectories.files.map {
                fileTree(it).exclude("salesforce/cdp/hyperdb/v1/**", "com/salesforce/datacloud/reference/**")
            }
        )
    )
}

tasks.test { finalizedBy(tasks.jacocoTestReport) }

tasks.jacocoTestReport { dependsOn(tasks.test) }

tasks.jacocoTestCoverageVerification { violationRules { rule { limit { minimum = "0.5".toBigDecimal() } } } }

tasks.jacocoTestReport {
    reports {
        xml.required = true
        csv.required = true
    }
    excludeGrpc()
}
