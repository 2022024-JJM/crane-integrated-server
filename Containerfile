# 서버 이미지. 보통은 compose로 빌드·실행한다: podman compose --profile app up -d --build
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
# 시각은 UTC로 다룬다(bootRun과 같음). OOM이면 프로세스를 끝내 재시작 정책이 새로 띄우게 한다.
ENTRYPOINT ["java", "-Duser.timezone=UTC", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/app.jar"]
