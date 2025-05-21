plugins {
    id("de.undercouch.download")
    id("com.google.osdetector")
}

val hyperApiVersion: String by project
val hyperZipPath = ".hyper/hyper-$hyperApiVersion.zip"
val hyperDir = ".hyperd"

tasks.register<de.undercouch.gradle.tasks.download.Download>("downloadHyper") {
    group = "hyper"

    val os = "os=${osdetector.os}, arch=${osdetector.arch}, classifier=${osdetector.classifier}"

    val osPart = when (osdetector.os) {
        "windows" -> "windows-x86_64-release-main"
        "linux" -> "linux-x86_64-release-main"
        "osx" -> when (osdetector.arch) {
            "aarch_64" -> "macos-arm64-release-main"
            else -> "macos-x86_64-release-main"
        }

        else -> throw GradleException("Unsupported os settings. $os")
    }

    val url = "https://downloads.tableau.com/tssoftware/tableauhyperapi-cxx-$osPart.$hyperApiVersion.zip"
    val zip = project.layout.projectDirectory.file(hyperZipPath)

    src(url)
    dest(zip)
    overwrite(false)

    inputs.property("hyperApiVersion", hyperApiVersion)
    inputs.property("osdetector.os", osdetector.os)
    inputs.property("osdetector.arch", osdetector.arch)

    outputs.file(zip)

    doLast {
        if (!zip.asFile.exists()) {
            throw GradleException("downloadHyper failed validation. zip=false, $os")
        }
    }
}

tasks.register<Copy>("extractHyper") {
    dependsOn("downloadHyper")

    val os = "os=${osdetector.os}, arch=${osdetector.arch}, classifier=${osdetector.classifier}"

    group = "hyper"
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    includeEmptyDirs = false

    from(zipTree(project.layout.projectDirectory.file(hyperZipPath))) {
        include("**/hyperd.exe", "**/hyperd", "**/*.dll", "**/*.dylib", "**/*.so")
    }

    into(project.layout.projectDirectory.dir(hyperDir))

    eachFile {
        relativePath = RelativePath(true, name)
    }

    filePermissions {
        unix("rwx------")
    }

    val hyperdDir = project.layout.projectDirectory.dir(hyperDir).asFileTree

    inputs.file(project.layout.projectDirectory.file(hyperZipPath))
    outputs.files(hyperdDir.files)

    doLast {

        val exe = hyperdDir.firstOrNull { it.name.contains("hyperd") }
            ?: throw GradleException("zip missing hyperd executable. $os, files=${hyperdDir.map { it.absolutePath }}")

        val lib = hyperdDir.firstOrNull { it.name.contains("tableauhyperapi") }
            ?: throw GradleException("zip missing hyperd library, $os, files=${hyperdDir.map { it.absolutePath }}")

        if (!exe.exists() || !lib.exists()) {
            throw GradleException("extractHyper failed validation. hyperd=${exe.exists()}, lib=${lib.exists()}, $os")
        }
    }
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
