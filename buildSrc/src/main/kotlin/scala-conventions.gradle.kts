plugins {
    id("java-base-conventions")
    id("scala")
    `java-library`
}

spotless {    
    scala {
        scalafmt()
    }
}