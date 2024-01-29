package io.kestra.plugin.pulsar;

import java.util.Map;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;

import io.kestra.core.runners.RunContext;

public class GenericProducer extends BaseProducer<GenericRecord> {

  enum SchemaTypeEnum {
    AVRO,
    JSON
  }

  Schema<GenericRecord> schema;

  String schemaString;

  SchemaTypeEnum schemaType;
  
  public GenericProducer(RunContext runContext, PulsarClient client, String schemaString, String schemaType) {
    super(runContext, client);
    
    this.schemaString = schemaString;
    this.schemaType =  SchemaTypeEnum.valueOf(schemaType.toUpperCase());
  }

  @Override
  protected ProducerBuilder<GenericRecord> GetProducerBuilder(PulsarClient client) {
    if (this.schemaString == null) { throw new IllegalArgumentException("Must pass a \"schemaString\" when the \"schemaType\" is \"AVRO\" or \"JSON\"");}
    
    SchemaDefinition<GenericRecord> schemaDef = SchemaDefinition
      .<GenericRecord>builder()
      // .withAlwaysAllowNull(false)
      .withJsonDef(this.schemaString)
      .build();
    
    this.schema = this.schemaType == SchemaTypeEnum.AVRO ? Schema.AVRO(schemaDef) : Schema.JSON(schemaDef);
    return client.newProducer(Schema.generic(schema.getSchemaInfo()));
  }

  @SuppressWarnings("unchecked")
  @Override
  protected TypedMessageBuilder<GenericRecord> CreateMessageWithValue(Map<String, Object> renderedMap) throws Exception {
    try {
      this.producer = this.producerBuilder.create();
      TypedMessageBuilder<GenericRecord> message = producer.newMessage();
      
      if (renderedMap.containsKey("value")) {
        GenericSchemaImpl schema = this.schemaType == SchemaTypeEnum.AVRO ? GenericAvroSchema.of(this.schema.getSchemaInfo()) : GenericJsonSchema.of(this.schema.getSchemaInfo());
        org.apache.pulsar.client.api.schema.GenericRecordBuilder record = schema.newRecordBuilder();
        Map<String, Object> value = (Map<String, Object>)renderedMap.get("value");
        value.forEach((k,v) -> record.set(k, v)); 
        message.value(record.build());
      }
  
      return message;
    } catch (Exception e) { throw e; }
  }
}
