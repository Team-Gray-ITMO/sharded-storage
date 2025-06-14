plugins {
    id("java")
    `java-library`
    application
    id("com.github.johnrengelman.shadow") apply true
}

private val mainClassName = "vk.itmo.teamgray.sharded.storage.test.api.TestApiApplication"

application {
    mainClass.set(mainClassName)
}

dependencies {
    api(project(":client"))

    //TODO Unify JUNIT ver and libs to fix test problems
    implementation("org.junit.jupiter:junit-jupiter-api:5.12.1")
    runtimeOnly("org.junit.jupiter:junit-jupiter-engine:5.12.1")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = mainClassName
    }
}

tasks.shadowJar {
    archiveClassifier.set("all")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes["Main-Class"] = mainClassName
    }

    mergeServiceFiles()

    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}

tasks.build {
    dependsOn(tasks.named("shadowJar"))
}

repositories {
    mavenCentral()
}
