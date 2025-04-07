plugins {
    id("java")
    application
    id("com.github.johnrengelman.shadow") version "8.1.1" apply false
}

group = "vk.itmo.teamgray.sharded.storage"
version = "1.0-SNAPSHOT"

allprojects {
    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "application")

    dependencies {
        implementation("org.slf4j:slf4j-api:2.0.16")
        implementation("org.slf4j:slf4j-simple:2.0.16")

        testImplementation("org.assertj:assertj-core:3.26.3")
        testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
    }

    tasks.test {
        useJUnitPlatform()
    }

    java {
        toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    }
}
