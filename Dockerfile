FROM gradle:8.13-jdk21 AS build
WORKDIR /app

COPY settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
RUN gradle --no-daemon dependencies

COPY common ./common
COPY master ./master
COPY node ./node
COPY discovery ./discovery
COPY client ./client

FROM build AS discovery-build
RUN gradle :discovery:build --no-daemon -x test

FROM build AS master-build
RUN gradle :master:build --no-daemon -x test

FROM build AS node-build
RUN gradle :node:build --no-daemon -x test

FROM eclipse-temurin:21-jre-jammy AS discovery-runtime
WORKDIR /app
COPY --from=discovery-build /app/discovery/build/libs/*.jar ./app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

FROM eclipse-temurin:21-jre-jammy AS master-runtime
WORKDIR /app
COPY --from=master-build /app/master/build/libs/*.jar ./app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

FROM eclipse-temurin:21-jre-jammy AS node-runtime
WORKDIR /app
COPY --from=node-build /app/node/build/libs/*.jar ./app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
