FROM openjdk:21-jdk-slim

COPY build/libs/node-all.jar /app/node.jar

ENTRYPOINT java -jar /app/node.jar
