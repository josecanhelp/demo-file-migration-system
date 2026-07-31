# Build stage: compiles the migrator jar with Maven. The pom is copied in
# before the sources so dependency resolution is cached in its own layer.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

# Runtime stage: just a JRE and the built jar, so the running image carries
# no build tooling. curl is added for the container healthcheck.
FROM eclipse-temurin:21-jre AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/target/migrator.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
