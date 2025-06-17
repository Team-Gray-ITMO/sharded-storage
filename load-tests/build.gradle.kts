plugins {
    id("java")
    application
}

val loadTestSourceSet = sourceSets.create("loadTest") {
    java.srcDir("src/loadTest/java")
    resources.srcDir("src/loadTest/resources")
    compileClasspath += sourceSets["main"].output
    compileClasspath += configurations["testRuntimeClasspath"].incoming.artifactView { lenient(true) }.files
    runtimeClasspath += output + compileClasspath
}

configurations {
    configurations["loadTestImplementation"].extendsFrom(configurations["testImplementation"])
    configurations["loadTestRuntimeOnly"].extendsFrom(configurations["testRuntimeOnly"])
}

dependencies {
    "loadTestImplementation"(projects.testApi)
}

tasks.named<ProcessResources>("processLoadTestResources") {
    duplicatesStrategy = DuplicatesStrategy.WARN
}

tasks.register<Test>("loadTest") {
    description = "Runs the load tests."
    group = "verification"

    testClassesDirs = loadTestSourceSet.output.classesDirs
    classpath = loadTestSourceSet.runtimeClasspath

    useJUnitPlatform()
}

repositories {
    mavenCentral()
}
