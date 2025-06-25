// import org.jreleaser.model.Active

plugins {
    id("hyper-conventions")
    id("base-conventions")
    id("version-conventions")
    id("com.diffplug.spotless")
    id("dev.iurysouza.modulegraph") version "0.12.0"
    // id("org.jreleaser") version "1.18.0"

}

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
    rootModulesRegex.set(":jdbc")
}

// jreleaser {
//     gitRootSearch = true
// }

//     deploy {
//         maven {
//             mavenCentral {
//                 active.set(Active.ALWAYS)
//                 register("sonatype") {
//                     active = Active.ALWAYS
//                     url = "https://central.sonatype.com/api/v1/publisher"
//                 }
//             }
//         }
//     }

//     signing {
//         active.set(Active.ALWAYS)
//         armored.set(true)
//     }

//     release {
//         github {
//             enabled = false
//             skipRelease = true
//             skipTag = true
//         }
//     }
// }
