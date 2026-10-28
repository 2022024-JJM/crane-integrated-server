# 서버 이미지. Docker·Podman 공용(Podman 은 Containerfile 다음으로 Dockerfile 을 찾는다).
# 로컬: <docker|podman> compose --profile app up -d --build / 배포: <docker|podman> compose build app
FROM docker.io/library/eclipse-temurin:25-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
RUN chmod +x gradlew && ./gradlew --no-daemon --quiet dependencies > /dev/null
COPY src src
RUN ./gradlew --no-daemon --quiet bootJar

FROM docker.io/library/eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /src/build/libs/*.jar app.jar
ENV LOG_PATH=/app/logs
EXPOSE 8080
# UTC 로 돈다(bootRun 과 같음). 힙은 컨테이너 메모리 상한의 75%. OOM 이면 종료해 재시작 정책에 맡긴다.
ENTRYPOINT ["java", "-Duser.timezone=UTC", "-XX:MaxRAMPercentage=75.0", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/app.jar"]
