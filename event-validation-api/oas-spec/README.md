# Event Validation API – OpenAPI Spec Module

This module packages the canonical OpenAPI 3.0 document for the Event Validation API. The spec lives at `src/main/resources/openapi/event-validation-api.yml` and is bundled into the `event-validation-api-oas` artifact so other teams can import it into their tooling or generate SDKs.

## Build
```bash
cd event-validation-api/oas-spec
mvn clean package
```

## Usage
- Publish the produced JAR to your internal artifact repository to distribute the spec.
- Use `openapi-generator-cli generate -i src/main/resources/openapi/event-validation-api.yml -g <language>` to generate clients or server stubs.
- Import the YAML into tools such as Stoplight Studio or Postman for interactive docs.
