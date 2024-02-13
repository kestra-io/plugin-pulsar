package io.kestra.plugin.pulsar;

import java.util.Map;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;

import io.kestra.core.runners.RunContext;

public class GenericRecordProducer extends AbstractProducer<GenericRecord> {
    
    private Schema<GenericRecord> schema;

    private final String schemaString;
    private final SchemaType schemaType;
    
    public GenericRecordProducer(RunContext runContext, PulsarClient client, String schemaString, SchemaType schemaType) {
        super(runContext, client);
        
        this.schemaString = schemaString;
        this.schemaType =  schemaType;
    }
    
    @Override
    protected ProducerBuilder<GenericRecord> getProducerBuilder(PulsarClient client) {
        if (this.schemaString == null) {
            throw new IllegalArgumentException("Must pass a \"schemaString\" when the \"schemaType\" is not null");
        }
        
        SchemaDefinition<GenericRecord> schemaDef = SchemaDefinition
            .<GenericRecord>builder()
            .withJsonDef(this.schemaString)
            .build();
        
        this.schema = this.schemaType == SchemaType.AVRO ? Schema.AVRO(schemaDef) : Schema.JSON(schemaDef);
        return client.newProducer(Schema.generic(schema.getSchemaInfo()));
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected TypedMessageBuilder<GenericRecord> createMessageWithValue(Map<String, Object> renderedMap) throws Exception {
        this.producer = this.producerBuilder.create();
        TypedMessageBuilder<GenericRecord> message = producer.newMessage();

        if (renderedMap.containsKey("value")) {
            GenericSchemaImpl schema = this.schemaType == SchemaType.AVRO ? GenericAvroSchema.of(this.schema.getSchemaInfo()) : GenericJsonSchema.of(this.schema.getSchemaInfo());
            org.apache.pulsar.client.api.schema.GenericRecordBuilder record = schema.newRecordBuilder();
            Map<String, Object> value = (Map<String, Object>)renderedMap.get("value");
            value.forEach((k,v) -> record.set(k, v));
            message.value(record.build());
        }

        return message;
    }
}
