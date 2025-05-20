FROM openjdk:21-jdk-slim

COPY build/libs/discovery-all.jar /app/discovery.jar

ENTRYPOINT java -jar /app/discovery.jar
