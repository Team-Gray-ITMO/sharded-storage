FROM openjdk:21-jdk-slim

COPY build/libs/master-all.jar /app/master.jar

ENTRYPOINT java -jar /app/master.jar
