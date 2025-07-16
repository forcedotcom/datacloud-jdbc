plugins {
    id("java-base-conventions")
    `java-library`
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withJavadocJar()
    withSourcesJar()
}

tasks.withType<JavaCompile> {
    javaCompiler = javaToolchains.compilerFor {
        languageVersion = JavaLanguageVersion.of(8)
    }
    options.encoding = "UTF-8"
    options.setIncremental(true)
    // This only works if we're on a newer toolchain, but java 8 is faster to build while we use lombok for "val"
    // options.release.set(8)
}

tasks.withType<Javadoc> {
    options.encoding = "UTF-8"
    (options as StandardJavadocDocletOptions).apply {
        addStringOption("Xdoclint:none", "-quiet")
        addBooleanOption("html5", true)
    }
}

spotless {
    java {
        target("src/main/java/**/*.java", "src/test/java/**/*.java")
        palantirJavaFormat("2.62.0")
        formatAnnotations()
        importOrder()
        removeUnusedImports()
        licenseHeaderFile(rootProject.file("license-header.txt"))
    }
}
