# How to use the Pulsar plugin

Produce and consume messages on Apache Pulsar topics from Kestra flows, with support for schemas, subscriptions, and non-durable readers.

## Common properties

Set `uri` to your Pulsar service URL (`pulsar://` for plain, `pulsar+ssl://` for TLS). For token authentication, set `authenticationToken`. For mutual TLS, provide `tlsOptions.cert`, `tlsOptions.key`, and `tlsOptions.ca` as base64-encoded PEM content. Apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults) and store secrets in [secrets](https://kestra.io/docs/concepts/secret).

## Tasks

`Produce` publishes messages to a `topic` — pass messages via `from` and set `serializer` (`STRING`, `JSON`, or `BYTES`). Control throughput with `compressionType` and producer behavior with `accessMode`.

`Consume` reads from one or more topics using a named `subscriptionName`. Set `initialPosition` (`Earliest` or `Latest`) and `subscriptionType` (`Exclusive`, `Shared`, or `Failover`). Bound the batch with `maxRecords` or `maxDuration`. Set `deserializer` to match the producer's serializer.

`Reader` provides non-durable, non-subscription access to a topic — useful for replaying messages without affecting consumer group offsets. Use `since` to start from a relative time offset or `messageId` to start from a specific message.

Enable schema validation with `schemaType` (`AVRO` or `JSON`) and provide the schema definition in `schemaString`.

`Trigger` polls on a schedule (default 60 seconds) and starts one execution per batch. `RealtimeTrigger` starts one execution per message. Use `Trigger` for scheduled batch processing and `RealtimeTrigger` for per-message workflows.
