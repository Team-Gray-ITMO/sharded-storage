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
RUN gradle :node:build --no-daemon

# Run stage
FROM openjdk:21-jdk-slim
WORKDIR /app
COPY --from=build /app/node/build/libs/node-all.jar /app/node.jar
ENTRYPOINT ["java", "-jar", "/app/node.jar"]