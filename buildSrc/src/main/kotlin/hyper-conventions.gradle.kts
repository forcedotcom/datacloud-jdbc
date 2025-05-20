plugins {
    id("de.undercouch.download")
    id("com.google.osdetector")
}

val hyperApiVersion: String by project
val hyperZipPath = ".hyper/hyper-$hyperApiVersion.zip"
val hyperDir = ".hyperd"

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
    dest(project.layout.projectDirectory.file(hyperZipPath))
    overwrite(false)
    
    inputs.property("hyperApiVersion", hyperApiVersion)
    inputs.property("osdetector.os", osdetector.os)
    outputs.file(project.layout.projectDirectory.file(hyperZipPath))
}

tasks.register<Copy>("extractHyper") {
    dependsOn("downloadHyper")

    group = "hyper"
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    includeEmptyDirs = false

    from(zipTree(project.layout.projectDirectory.file(hyperZipPath))) {
        include("**/lib/hyper/hyperd")
        include("**/lib/hyper/hyperd.exe")
        include("**/lib/**/*.dylib")
        include("**/lib/**/*.dll")
        include("**/lib/**/*.so")
    }

    eachFile {
        relativePath = RelativePath(true, name)
    }

    into(project.layout.projectDirectory.dir(hyperDir))

    filePermissions {
        unix("rwx------")
    }
    
    inputs.file(project.layout.projectDirectory.file(hyperZipPath))
    outputs.dir(project.layout.projectDirectory.dir(hyperDir))

}

tasks.register<Exec>("hyperd") {
    dependsOn("extractHyper")
    group = "hyper"

    val name = when (osdetector.os) {
        "windows" -> "hyperd.exe"
        else -> "hyperd"
    }

    val executable = project.layout.projectDirectory.dir(hyperDir).file(name).asFile.absolutePath
    val config = project.project(":jdbc-core").file("src/test/resources/hyper.yaml")

    commandLine(executable)
    args("--config", config.absolutePath, "run")

    inputs.files(executable, config)
    inputs.property("os", osdetector.os)
}
