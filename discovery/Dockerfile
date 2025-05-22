# Build stage
FROM gradle:8.13-jdk21 AS build
WORKDIR /app

COPY settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle

# copy all modules
COPY discovery ./discovery
COPY common ./common
COPY node ./node
COPY client ./client

# build
RUN gradle :discovery:build --no-daemon -x test

# Run stage
FROM openjdk:21-jdk-slim
WORKDIR /app
COPY --from=build /app/discovery/build/libs/discovery-all.jar /app/discovery.jar
ENTRYPOINT ["java", "-jar", "/app/discovery.jar"]
