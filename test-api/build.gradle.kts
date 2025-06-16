plugins {
    id("java")
    `java-library`
    application
}

private val mainClassName = "vk.itmo.teamgray.sharded.storage.test.api.TestApiApplication"

application {
    mainClass.set(mainClassName)
}

dependencies {
    api(projects.client)

    api(libs.assertj.core)
    api(libs.mockito)
    api(libs.assertj.core)
    api(libs.junit.jupiter.api)
    api(libs.junit.platform.launcher)
    api(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
}

repositories {
    mavenCentral()
}
