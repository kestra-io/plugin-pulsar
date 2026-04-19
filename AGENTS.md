# Kestra Pulsar Plugin

## What

- Provides plugin components under `io.kestra.plugin.pulsar`.
- Includes classes such as `Consume`, `SerdeType`, `ByteArrayProducer`, `GenericRecordProducer`.

## Why

- What user problem does this solve? Teams need to publish, consume, and trigger workflows with Apache Pulsar from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Apache Pulsar steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Apache Pulsar.

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
