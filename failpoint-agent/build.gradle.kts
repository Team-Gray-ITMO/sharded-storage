import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("java")
    application
    alias(libs.plugins.shadow)
}

private val mainClassName = "vk.itmo.teamgray.sharded.storage.test.api.FailpointAgentApplication"

application {
    mainClass.set(mainClassName)
}

dependencies {
    implementation(projects.common)

    implementation(libs.byte.buddy)
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<ShadowJar>().configureEach {
    manifest {
        attributes(
            "Premain-Class" to "vk.itmo.teamgray.failpoint.agent.FailpointAgent",
            "Can-Redefine-Classes" to "true",
            "Can-Retransform-Classes" to "true"
        )
    }
}

repositories {
    mavenCentral()
}
