package io.kestra.plugin.pulsar;

import java.util.Map;

import org.apache.pulsar.client.api.*;

import io.kestra.core.runners.RunContext;

public class ByteProducer extends AbstractProducer<byte[]>{
    
    private SerdeType serializer;
    
    public ByteProducer(RunContext runContext, PulsarClient client, SerdeType serializer) {
        super(runContext, client);
        this.serializer = serializer;
    }
    
    @Override
    protected ProducerBuilder<byte[]> getProducerBuilder(PulsarClient client) {
        return client.newProducer();
    }
    
    @Override
    protected TypedMessageBuilder<byte[]> createMessageWithValue(Map<String, Object> renderedMap) throws Exception {
        try {
            this.producer = this.producerBuilder.create();
            TypedMessageBuilder<byte[]> message = this.producer.newMessage();
            if (renderedMap.containsKey("value")) {
                message.value(this.serializer.serialize(renderedMap.get("value")));
            }
            
            return message;
        } catch (Exception e) { throw e; }
    }
    
}
