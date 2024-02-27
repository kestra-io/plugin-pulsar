package io.kestra.plugin.pulsar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.client.impl.schema.generic.*;

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
          // For AVRO schemas we need to load the native schema to check if we need to denest any records types
          org.apache.pulsar.shade.org.apache.avro.Schema nativeSchema = this.schemaType == SchemaType.AVRO ? ((org.apache.pulsar.shade.org.apache.avro.Schema)this.schema.getNativeSchema().get()) : null;

          
          GenericSchemaImpl schema = this.schemaType == SchemaType.AVRO ? GenericAvroSchema.of(this.schema.getSchemaInfo()) : GenericJsonSchema.of(this.schema.getSchemaInfo());
          GenericRecordBuilder record = schema.newRecordBuilder();
            Map<String, Object> value = (Map<String, Object>)renderedMap.get("value");
            value.forEach((k,v) -> record.set(k, this.schemaType == SchemaType.AVRO ? denestRecord(v, nativeSchema.getField(k).schema()): v));
            message.value(record.build());
        }

        return message;
    }
    
    @SuppressWarnings("unchecked")
    private Object denestRecord(Object value, org.apache.pulsar.shade.org.apache.avro.Schema schema) {      
      switch(schema.getType()) {
        case RECORD:
          // This doesn't work for user-defined class objects. However, given values are inputs from Kestra they should all be maps
          org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord record = new org.apache.pulsar.shade.org.apache.avro.generic.GenericData.Record(schema);
          for (Map.Entry<String, Object> entry : ((Map<String, Object>)value).entrySet())
            record.put(entry.getKey(), denestRecord(entry.getValue(), schema.getField(entry.getKey()).schema()));
          return record;
        case ARRAY:
          List<Object> list = new ArrayList<Object>();
          for (Object x : (Iterable<Object>)value) {
            list.add(denestRecord(x, schema.getElementType()));
          }
          return list;
        case MAP:
          HashMap<String, Object> map = new HashMap<>();
          for (Map.Entry<String, Object> entry : ((Map<String, Object>)value).entrySet())
            map.put(entry.getKey(), denestRecord(entry.getValue(), schema.getValueType()));
          return map;
        default:
          return value;
      }
    }
}
