plugins {
    id("java")
    id("com.github.johnrengelman.shadow") apply true
}

private val mainClassName = "vk.itmo.teamgray.sharded.storage.discovery.DiscoveryApplication"

application {
    mainClass.set(mainClassName)
}

dependencies {
    implementation(project(":common"))
    implementation("org.jetbrains:annotations:26.0.2")

    testImplementation("org.mockito:mockito-core:5.17.0")
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

