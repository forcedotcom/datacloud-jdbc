plugins {
    `java-library`
}

val lombok = "org.projectlombok:lombok:1.18.36"

dependencies {
    compileOnly(lombok)
    annotationProcessor(lombok)
    
    testCompileOnly(lombok)
    testAnnotationProcessor(lombok)
}

val delombok = tasks.register<JavaExec>("delombok") {
    group = "documentation"
    description = "Delomboks the source code for Javadoc generation"
    
    classpath = configurations.getByName("annotationProcessor")
    mainClass.set("lombok.launch.Main")
    
    val srcDir = file("${project.projectDir}/src/main/java")
    val outputDir = file("${buildDir}/delombok")
    
    args = listOf(
        "delombok",
        srcDir.toString(),
        "--output", outputDir.toString(),
        "--format", "pretty"
    )
    
    inputs.dir(srcDir)
    outputs.dir(outputDir)
}

tasks.named<Javadoc>("javadoc") {
    dependsOn(delombok)
    source = delombok.get().outputs.files.asFileTree
    
    (options as StandardJavadocDocletOptions).apply {
        addStringOption("Xdoclint:none", "-quiet")
        addBooleanOption("html5", true)
    }
    
    isFailOnError = false
}
