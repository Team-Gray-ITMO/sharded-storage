plugins {
    id("java")
    id("jacoco")
    alias(libs.plugins.qameta.allure)
    alias(libs.plugins.shadow)
}

private val mainClassName = "vk.itmo.teamgray.sharded.storage.master.MasterApplication"

application {
    mainClass.set(mainClassName)
}

dependencies {
    implementation(projects.common)
    implementation(projects.node)

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

val serviceClassDirectories: ConfigurableFileCollection = files(
    fileTree(project.layout.buildDirectory.dir("classes/java/main")).matching {
        include(project.ext["coverageIncludes"] as List<String>)
        exclude(project.ext["coverageExcludes"] as List<String>)
    }
)

tasks.jacocoTestReport {
    dependsOn(tasks.test)

    reports {
        xml.required.set(true)
        html.required.set(true)
        csv.required.set(false)
    }

    classDirectories.setFrom(serviceClassDirectories)

    doLast {
        val report = file("${reports.html.outputLocation.get()}/index.html")
        println("JaCoCo report: ${report.toURI()}")
    }
}

tasks.jacocoTestCoverageVerification {
    dependsOn(tasks.jacocoTestReport)

    violationRules {
        rule {
            limit {
                minimum = project.ext["coverageMinimum"] as BigDecimal
                counter = "LINE"
                value = "COVEREDRATIO"
            }
            includes = (project.ext["coverageIncludes"] as List<String>).map {
                it.replace("**/", "*..")
                    .replace("**", "*")
                    .replace("/*/", ".*.")
            }
        }
    }

    classDirectories.setFrom(serviceClassDirectories)
}

tasks.test {
    finalizedBy(tasks.jacocoTestReport)
    finalizedBy(tasks.jacocoTestCoverageVerification)
}

tasks.jacocoTestReport {
    mustRunAfter(tasks.test)
}

tasks.jacocoTestCoverageVerification {
    mustRunAfter(tasks.jacocoTestReport)
}

