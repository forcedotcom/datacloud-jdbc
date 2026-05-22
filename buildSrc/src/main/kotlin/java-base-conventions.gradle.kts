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

// Exclude grpc-netty-shaded (pulled in transitively by grpcmock) to prevent it from winning
// gRPC provider discovery over our grpc-netty. The shaded variant has higher priority, and
// mixing its older transport with our grpc-core version causes channels to never terminate.
configurations.all {
    exclude(group = "io.grpc", module = "grpc-netty-shaded")
}

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

    // Iceberg sets -Darrow.enable_null_check_for_get=false on the JVM. With that flag off, Arrow's
    // VarCharVector/VarBinaryVector/FixedSizeBinaryVector/TimeStamp* getters skip the validity check
    // and return stale buffer bytes for null rows instead of null. Run every test task across the
    // build under that condition so the existing null-handling assertions cover the Iceberg scenario.
    // The true case we don't need to test explicitly as everything that works with false will also
    // work with true given the semantics of the setting.
    systemProperty("arrow.enable_null_check_for_get", "false")
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
