# Kestra Pulsar Plugin

## What

- Provides plugin components under `io.kestra.plugin.pulsar`.
- Includes classes such as `Consume`, `SerdeType`, `ByteArrayProducer`, `GenericRecordProducer`.

## Why

- This plugin integrates Kestra with Apache Pulsar.
- It provides tasks that publish, consume, and trigger workflows with Apache Pulsar.

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

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
