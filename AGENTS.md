# Kestra Pulsar Plugin

## What

Leverage Apache Pulsar messaging in Kestra data orchestration. Exposes 7 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Apache Pulsar, allowing orchestration of Apache Pulsar-based operations as part of data pipelines and automation workflows.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `pulsar`

Infrastructure dependencies (Docker Compose services):

- `manager`
- `pulsar`

### Key Plugin Classes

- `io.kestra.plugin.pulsar.AbstractProducer<T>`
- `io.kestra.plugin.pulsar.AbstractReader`
- `io.kestra.plugin.pulsar.Consume`
- `io.kestra.plugin.pulsar.Produce`
- `io.kestra.plugin.pulsar.Reader`
- `io.kestra.plugin.pulsar.RealtimeTrigger`
- `io.kestra.plugin.pulsar.Trigger`

### Project Structure

```
plugin-pulsar/
├── src/main/java/io/kestra/plugin/pulsar/
├── src/test/java/io/kestra/plugin/pulsar/
├── build.gradle
└── README.md
```

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
