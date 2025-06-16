plugins {
    id("java")
    `java-library`
    application
    alias(libs.plugins.shadow)
}

private val mainClassName = "vk.itmo.teamgray.sharded.storage.client.ClientApplication"

application {
    mainClass.set(mainClassName)
}

dependencies {
    api(projects.common)

    testImplementation(libs.assertj.core)
    testImplementation(libs.mockito)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.junit.platform.launcher)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
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
