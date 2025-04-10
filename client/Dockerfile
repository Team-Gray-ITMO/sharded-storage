# Build stage
FROM gradle:8.6-jdk21 AS build
WORKDIR /app

COPY settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle

# copy all modules
COPY master ./master
COPY common ./common
COPY node ./node
COPY client ./client

# build
RUN gradle :client:build --no-daemon

# Run stage
FROM openjdk:21-jdk-slim
WORKDIR /app
COPY --from=build /app/client/build/libs/client-all.jar /app/client.jar
ENTRYPOINT ["java", "-jar", "/app/client.jar"]