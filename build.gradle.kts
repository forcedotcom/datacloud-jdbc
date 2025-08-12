plugins {
    id("hyper-conventions")
    id("base-conventions")
    id("version-conventions")
    id("com.diffplug.spotless")
    id("dev.iurysouza.modulegraph") version "0.12.0"
    id("jacoco-report-aggregation")
}

repositories { mavenCentral() }

tasks.register("printVersion") {
    val projectVersion = version.toString()
    doLast {
        println(projectVersion)
    }
}

tasks.register("printHyperApiVersion") {
    val hyperVersion = project.findProperty("hyperApiVersion")?.toString() ?: "unknown"
    doLast {
        println(hyperVersion)
    }
}

subprojects {
    plugins.withId("java-conventions") {
        tasks.withType<Test>().configureEach {
            dependsOn(rootProject.tasks.named("extractHyper"))
        }
    }
}

moduleGraphConfig {
    readmePath.set("${rootDir}/DEVELOPMENT.md")
    heading.set("## Module Graph")
    rootModulesRegex.set("^:jdbc|:spark-datasource$")
}

dependencies {
    jacocoAggregation(project(":jdbc"))
    jacocoAggregation(project(":jdbc-core"))
    jacocoAggregation(project(":jdbc-grpc"))
    jacocoAggregation(project(":jdbc-http"))
    jacocoAggregation(project(":jdbc-util"))
    jacocoAggregation(project(":jdbc-reference"))
    jacocoAggregation(project(":spark-datasource"))
}

reporting {
    reports {
        val testCodeCoverageReport by
        creating(JacocoCoverageReport::class) {
            testSuiteName.set("test")
            reportTask
                .get()
                .classDirectories
                .setFrom(
                    reportTask.get().classDirectories.map {
                        fileTree(it).matching {
                            exclude("salesforce/cdp/hyperdb/v1/**", "com/salesforce/datacloud/reference/**")
                        }
                    }
                )
            reportTask.get().reports.apply {
                xml.required.set(true)
                csv.required.set(true)
            }
        }

    }
}

tasks.named("build") { dependsOn(tasks.named("testCodeCoverageReport")) }
