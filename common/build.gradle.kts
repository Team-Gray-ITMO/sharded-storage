import com.google.protobuf.gradle.id

plugins {
    id("java")
    id("java-library")
    alias(libs.plugins.google.protobuf)
}

group = "vk.itmo.teamgray.sharded.storage"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    api(libs.grpc.netty.shaded)
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)

    api(libs.slf4j.api)
    api(libs.slf4j.simple)

    api(libs.protobuf.java)

    compileOnly(libs.javax.annotation.api)
}

sourceSets {
    main {
        proto {
            srcDir("src/main/proto")
        }
        java {
            srcDir("src/main/java")
        }
    }
    test {
        proto {
            srcDir("src/test/proto")
        }
        java {
            srcDir("src/test/java")
        }
    }
}

tasks {
    processResources {
        duplicatesStrategy = DuplicatesStrategy.WARN
    }

    withType<Copy>().configureEach {
        duplicatesStrategy = DuplicatesStrategy.WARN
    }
}

protobuf {
    protoc {
        artifact = getDependencyAsString(libs.protobuf.protoc)
    }

    plugins {
        id("grpc") {
            artifact = getDependencyAsString(libs.grpc.protoc.gen.java)
        }
    }

    generateProtoTasks {
        all().configureEach {
            plugins {
                id("grpc")
            }

            doFirst {
                delete(generatedFilesBaseDir)
            }
        }
    }
}

tasks.test {
    useJUnitPlatform()
}

private fun getDependencyAsString(provider: Provider<MinimalExternalModuleDependency>): String {
    val dependency = provider.get()

    return dependency.group + ":" + dependency.name + ":" + dependency.version
}
