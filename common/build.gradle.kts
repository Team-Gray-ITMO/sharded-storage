import com.google.protobuf.gradle.id

plugins {
    `java-library`
    id("java")
    id("com.google.protobuf") version "0.9.4"
}

group = "vk.itmo.teamgray.sharded.storage"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

//TODO Extract common versions variables for gRPC
dependencies {
    api("io.grpc:grpc-netty-shaded:1.71.0")
    api("io.grpc:grpc-protobuf:1.71.0")
    api("io.grpc:grpc-stub:1.71.0")

    api("com.google.protobuf:protobuf-java:4.30.1")

    compileOnly("javax.annotation:javax.annotation-api:1.3.2")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.12.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.12.1")
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
        artifact = "com.google.protobuf:protoc:4.30.1"
    }

    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.71.0"
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
