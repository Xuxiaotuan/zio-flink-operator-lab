FROM eclipse-temurin:17-jre

WORKDIR /app
COPY target/scala-3.3.5/zio-flink-operator-lab-assembly-0.1.0-SNAPSHOT.jar /app/zio-flink-operator-lab.jar

USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/zio-flink-operator-lab.jar"]
