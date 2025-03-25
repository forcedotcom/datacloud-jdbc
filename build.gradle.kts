plugins {
    id("base-conventions")
    id("de.undercouch.download")
    id("com.google.osdetector")
    id("com.diffplug.spotless")
}

val hyperApiVersion: String by project
val hyperZipName = "hyper-$hyperApiVersion.zip"

tasks.register<de.undercouch.gradle.tasks.download.Download>("downloadHyper") {
    group = "hyper"
    val urlBase = when (osdetector.os) {
        "windows" -> "https://downloads.tableau.com/tssoftware/tableauhyperapi-cxx-windows-x86_64-release-main"
        "osx" -> "https://downloads.tableau.com/tssoftware/tableauhyperapi-cxx-macos-arm64-release-main"
        "linux" -> "https://downloads.tableau.com/tssoftware/tableauhyperapi-cxx-linux-x86_64-release-main"
        else -> throw GradleException("Unsupported os settings. os=${osdetector.os}, arch=${osdetector.arch}, release=${osdetector.release}, classifier=${osdetector.classifier}}")
    }

    val url = "$urlBase.$hyperApiVersion.zip"

    src(url)
    dest(project.layout.buildDirectory.file(hyperZipName))
    overwrite(false)
}

tasks.register<Copy>("hyper") {
    dependsOn("downloadHyper")

    group = "hyper"
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    includeEmptyDirs = false

    from(zipTree(project.layout.buildDirectory.file(hyperZipName))) {
        include("**/lib/hyper/hyperd")
        include("**/lib/hyper/hyperd.exe")
        include("**/lib/**/*.dylib")
        include("**/lib/**/*.dll")
        include("**/lib/**/*.so")
    }

    eachFile {
        relativePath = RelativePath(true, name)
    }

    into(project.layout.buildDirectory.dir("hyperd"))

    filePermissions {
        unix("rwx------")
    }
}

tasks.register<Delete>("cleanupHyperZip") {
    group = "hyper"
    delete(
        project.layout.buildDirectory.file(hyperZipName),
        project.layout.buildDirectory.dir("hyperd"),
    )
}

subprojects {
    plugins.withId("java-conventions") {
        tasks.withType<Test>().configureEach {
            dependsOn(rootProject.tasks.named("hyper"))
        }
    }
}

//spotless {
////    val licenseHeaderTxt = layout.projectDirectory.file("license-header.txt")
//
//    ratchetFrom("origin/main")
//
////    protobuf {
////        buf()
////        ratchetFrom("origin/main")
////        licenseHeaderFile(licenseHeaderTxt)
////    }
//
//    format("misc") {
//        target(".gitattributes", ".gitignore")
//
//        trimTrailingWhitespace()
//        leadingSpacesToTabs()
//        endWithNewline()
//    }
//}