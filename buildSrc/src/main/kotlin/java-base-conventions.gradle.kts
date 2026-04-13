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

val nettyVersion = extensions.getByType<VersionCatalogsExtension>()
    .named("libs")
    .findVersion("netty")
    .get()
    .requiredVersion

dependencies {
    constraints {
        // gRPC and Arrow pull in Netty 4.1.x transitively. These constraints enforce a minimum
        // version to fix security vulnerabilities. Keep the version in gradle/libs.versions.toml.
        listOf(
            "io.netty:netty-buffer",
            "io.netty:netty-codec",
            "io.netty:netty-codec-http",
            "io.netty:netty-codec-http2",
            "io.netty:netty-common",
            "io.netty:netty-handler",
            "io.netty:netty-resolver",
            "io.netty:netty-transport",
            "io.netty:netty-transport-native-unix-common",
        ).forEach { module ->
            implementation("$module:$nettyVersion")
        }
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
