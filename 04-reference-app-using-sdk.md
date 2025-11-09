# Plan: Reference Spring Boot Producer App Using the SDK

## 0. Goal
Provide a minimal Spring Boot app that uses `webhooks-sdk` to publish real events end-to-end.

## 1. Features
- REST endpoint `POST /demo/customers/{id}` to simulate updates.
- Loads schema once, caches, and publishes sample event.
- Configuration via `application.yaml` (endpoint, auth, tenant).
- Integration tests that run against localstack/MSK test container.

## 2. Project Layout
```
reference-app/
  src/main/java/.../DemoController.java
  src/test/java/.../DemoE2eTest.java
  resources/application.yaml
```
- Include runnable `docker-compose` with Kafka test container.

## 3. Acceptance
- [ ] One-click run sends event → ingress topic
- [ ] Example shows error handling + retries
