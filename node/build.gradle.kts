plugins {
    id("java")
}

private val mainClassName = "vk.itmo.teamgray.sharded.storage.node.NodeApplication"

application {
    mainClass.set(mainClassName)
}

dependencies {
    implementation(project(":common"))
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = mainClassName
    }
}

tasks.register<Jar>("fatJar") {
    archiveClassifier.set("all")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    from(sourceSets.main.get().output)

    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
    })

    manifest {
        attributes["Main-Class"] = mainClassName
    }
}

tasks.build {
    dependsOn(tasks.named("fatJar"))
}

