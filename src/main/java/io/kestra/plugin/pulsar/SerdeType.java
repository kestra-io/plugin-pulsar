package io.kestra.plugin.pulsar;

import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@io.swagger.v3.oas.annotations.media.Schema(
    title = "Serializer / Deserializer used for the value."
)
public enum SerdeType {
    STRING,
    JSON,
    BYTES;

    public Object deserialize(byte[] payload) throws IOException {
        if (this == SerdeType.JSON) {
            return JacksonMapper.ofJson(false).readValue(payload, Object.class);
        } else if (this == SerdeType.STRING) {
            return new String(payload, StandardCharsets.UTF_8);
        } else {
            return payload;
        }
    }

    public byte[] serialize(Object message) throws IOException {
        if (this == SerdeType.JSON) {
            return JacksonMapper.ofJson(false).writeValueAsBytes(message);
        } else if (this == SerdeType.STRING) {
            return message.toString().getBytes(StandardCharsets.UTF_8);
        } else {
            return (byte[]) message;
        }
    }
}
