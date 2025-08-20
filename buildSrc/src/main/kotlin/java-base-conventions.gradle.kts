plugins {
    id("base-conventions")
    `java-library`
    jacoco
}

group = "com.salesforce.datacloud"

repositories {
    mavenLocal()
    maven {
        url = uri("https://repo.maven.apache.org/maven2/")
    }
}

tasks.withType<Test>().configureEach {
    javaLauncher = javaToolchains.launcherFor {
        languageVersion = JavaLanguageVersion.of(8)
    }

    useJUnitPlatform()

    testLogging {
        events("passed", "skipped", "failed")
        showExceptions = true
        showStandardStreams = true
        showStackTraces = true
    }

    jvmArgs("-Xmx1g", "-Xms512m")

    maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).takeIf { it > 0 } ?: 1
}

fun JacocoReportBase.excludeGrpc() {
    classDirectories.setFrom(
        files(
            classDirectories.files.map {
                fileTree(it).exclude(
                    "salesforce/cdp/hyperdb/v1/**", // excludes gRPC gen-code from coverage
                    "com/salesforce/datacloud/reference/**", // excludes test harness code from coverage
                )
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
