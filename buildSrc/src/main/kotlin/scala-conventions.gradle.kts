plugins {
    id("java-conventions")
    id("scala")
}

spotless {    
    scala {
        scalafmt()
    }
}