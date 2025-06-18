FROM gradle:8.14.1-jdk21 AS build
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
COPY --from=discovery-build /app/discovery/build/libs/discovery-all.jar ./discovery.jar
ENTRYPOINT ["java", "-jar", "/app/discovery.jar"]

FROM eclipse-temurin:21-jre-jammy AS master-runtime
WORKDIR /app
COPY --from=master-build /app/master/build/libs/master-all.jar ./master.jar
ENTRYPOINT ["java", "-jar", "/app/master.jar"]

FROM eclipse-temurin:21-jre-jammy AS node-runtime
WORKDIR /app
COPY --from=node-build /app/node/build/libs/node-all.jar ./node.jar
ENTRYPOINT ["java", "-jar", "/app/node.jar"]
