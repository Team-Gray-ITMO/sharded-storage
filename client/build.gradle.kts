plugins {
    id("java")
    application
    id("com.github.johnrengelman.shadow") apply true
}

private val mainClassName = "vk.itmo.teamgray.sharded.storage.client.ClientApplication"

application {
    mainClass.set(mainClassName)
}

dependencies {
    implementation(project(":common"))
    implementation("org.jetbrains:annotations:24.0.0")
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
