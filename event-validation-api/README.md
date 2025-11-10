# Event Validation API (Multi-Module)

This directory now contains two Maven modules that ship the runtime service and its OpenAPI contract:

| Module | Description |
| --- | --- |
| `service/` | Spring Boot WebFlux implementation that validates incoming events, persists schema metadata, and publishes to MSK. |
| `oas-spec/` | Canonical OpenAPI 3.0 specification for the API, packaged as an artifact that downstream teams can consume. |

## Building

```bash
cd event-validation-api
mvn clean install
```

- Build only the service: `mvn clean package -pl service`
- Build only the spec artifact: `mvn clean package -pl oas-spec`

## Running the service

```bash
cd event-validation-api
mvn spring-boot:run -pl service -am \
  -Dspring-boot.run.jvmArguments="-Dspring.profiles.active=dev"
```

See `service/README.md` for configuration details and required environment variables.

## Working with the OpenAPI spec

The generated artifact `event-validation-api-oas` exposes `src/main/resources/openapi/event-validation-api.yml`. You can import it into Stoplight, Postman, or use it as input to code generators (e.g., `openapi-generator-cli`).
