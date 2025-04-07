plugins {
    id("java")
    id("jacoco")
    id("io.qameta.allure") version "2.12.0"
    id("com.github.johnrengelman.shadow") apply true
}

private val mainClassName = "vk.itmo.teamgray.sharded.storage.node.NodeApplication"

application {
    mainClass.set(mainClassName)
}

dependencies {
    implementation(project(":common"))
    implementation("org.jetbrains:annotations:26.0.2")
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

