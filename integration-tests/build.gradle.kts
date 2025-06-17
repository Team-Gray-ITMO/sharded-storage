import org.gradle.kotlin.dsl.invoke

plugins {
    id("java")
    application
}

val integrationTestSourceSet = sourceSets.create("integrationTest") {
    java.srcDir("src/integrationTest/java")
    resources.srcDir("src/integrationTest/resources")
    compileClasspath += sourceSets["main"].output
    compileClasspath += configurations["testRuntimeClasspath"].incoming.artifactView { lenient(true) }.files
    runtimeClasspath += output + compileClasspath
}


configurations {
    configurations["integrationTestImplementation"].extendsFrom(configurations["testImplementation"])
    configurations["integrationTestRuntimeOnly"].extendsFrom(configurations["testRuntimeOnly"])
}

configurations.named("integrationTestRuntimeOnly") {
    resolutionStrategy {
        // your runtime resolution rules here
    }
}

dependencies {
    "integrationTestImplementation"(projects.testApi)
}

tasks.named<ProcessResources>("processIntegrationTestResources") {
    duplicatesStrategy = DuplicatesStrategy.WARN
}

tasks.register<Test>("integrationTest") {
    description = "Runs the integration tests."
    group = "verification"

    testClassesDirs = integrationTestSourceSet.output.classesDirs
    classpath = integrationTestSourceSet.runtimeClasspath

    useJUnitPlatform()
}

repositories {
    mavenCentral()
}
