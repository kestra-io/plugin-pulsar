package io.kestra.plugin.pulsar;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class EncryptionTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @TempDir
    Path directory;

    @Test
    void inlinePem() throws Exception {
        var pair = keyPair();
        roundTrip(pem("PUBLIC KEY", pair.getPublic().getEncoded()), pem("PRIVATE KEY", pair.getPrivate().getEncoded()));
    }

    @Test
    void dataUrl() throws Exception {
        var pair = keyPair();
        roundTrip(dataUrl(pem("PUBLIC KEY", pair.getPublic().getEncoded())), dataUrl(pem("PRIVATE KEY", pair.getPrivate().getEncoded())));
    }

    @Test
    void fileUrl() throws Exception {
        var pair = keyPair();
        var publicKey = Files.writeString(directory.resolve("public.pem"), pem("PUBLIC KEY", pair.getPublic().getEncoded()));
        var privateKey = Files.writeString(directory.resolve("private.pem"), pem("PRIVATE KEY", pair.getPrivate().getEncoded()));
        roundTrip(publicKey.toUri().toString(), privateKey.toUri().toString());
    }

    @Test
    void schemaAwareProducer() throws Exception {
        var pair = keyPair();
        roundTrip(pem("PUBLIC KEY", pair.getPublic().getEncoded()), pem("PRIVATE KEY", pair.getPrivate().getEncoded()), true);
    }

    private void roundTrip(String publicKey, String privateKey) throws Exception {
        roundTrip(publicKey, privateKey, false);
    }

    @SuppressWarnings("unchecked")
    private void roundTrip(String publicKey, String privateKey, boolean schemaAware) throws Exception {
        var runContext = runContextFactory.of(Map.of("publicKey", publicKey, "privateKey", privateKey));
        var topic = "encryption_" + IdUtils.create();
        var builder = Produce.builder()
            .id(IdUtils.create())
            .type(Produce.class.getName())
            .uri(Property.ofValue("pulsar://localhost:26650"))
            .topic(Property.ofValue(topic))
            .encryptionKey(Property.ofExpression("{{ publicKey }}"))
            .from(Map.of("value", "confidential"));
        if (schemaAware) {
            builder.schemaType(Property.ofValue(SchemaType.JSON))
                .schemaString(Property.ofValue("{\"type\":\"record\",\"name\":\"Message\",\"fields\":[{\"name\":\"secret\",\"type\":\"string\"}]}"))
                .from(Map.of("value", Map.of("secret", "confidential")));
        }
        var produce = builder.build();
        assertEquals(1, produce.run(runContext).getMessagesCount());

        try (
            var client = PulsarClient.builder().serviceUrl("pulsar://localhost:26650").build();
            var consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName(IdUtils.create())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .cryptoFailureAction(ConsumerCryptoFailureAction.CONSUME)
                .subscribe()
        ) {
            var encrypted = consumer.receive(10, TimeUnit.SECONDS);
            assertNotNull(encrypted);
            assertTrue(encrypted.getEncryptionCtx().isPresent());
            assertFalse(encrypted.getEncryptionCtx().orElseThrow().getKeys().isEmpty());
            assertNotEquals("confidential", new String(encrypted.getData(), StandardCharsets.UTF_8));
        }

        var consume = Consume.builder()
            .id(IdUtils.create())
            .type(Consume.class.getName())
            .uri(Property.ofValue("pulsar://localhost:26650"))
            .topic(topic)
            .subscriptionName(Property.ofValue(IdUtils.create()))
            .deserializer(Property.ofValue(schemaAware ? SerdeType.JSON : SerdeType.STRING))
            .encryptionKey(Property.ofExpression("{{ privateKey }}"))
            .maxRecords(Property.ofValue(1))
            .maxDuration(Property.ofValue(Duration.ofSeconds(15)))
            .build();
        var output = consume.run(runContext);
        assertEquals(1, output.getMessagesCount());
        var records = new ArrayList<Map<String, Object>>();
        try (var stream = storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())) {
            FileSerde.read(stream, record -> records.add((Map<String, Object>) record));
        }
        assertEquals(schemaAware ? Map.of("secret", "confidential") : "confidential", records.getFirst().get("value"));
    }

    @Test
    void invalidKeyFailsInsteadOfPublishingPlaintext() {
        var runContext = runContextFactory.of(Map.of());
        var produce = Produce.builder()
            .id(IdUtils.create())
            .type(Produce.class.getName())
            .uri(Property.ofValue("pulsar://localhost:26650"))
            .topic(Property.ofValue("invalid_key_" + IdUtils.create()))
            .encryptionKey(Property.ofValue("data:application/x-pem-file;base64,aW52YWxpZA=="))
            .from(Map.of("value", "confidential"))
            .build();
        assertThrows(Exception.class, () -> produce.run(runContext));
    }

    @Test
    void unencryptedMessagesStillWork() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var topic = "plaintext_" + IdUtils.create();
        var produce = Produce.builder()
            .id(IdUtils.create())
            .type(Produce.class.getName())
            .uri(Property.ofValue("pulsar://localhost:26650"))
            .topic(Property.ofValue(topic))
            .from(Map.of("value", "plain"))
            .build();
        assertEquals(1, produce.run(runContext).getMessagesCount());
        try (
            var client = PulsarClient.builder().serviceUrl("pulsar://localhost:26650").build();
            var consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName(IdUtils.create())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()
        ) {
            var message = consumer.receive(10, TimeUnit.SECONDS);
            assertNotNull(message);
            assertTrue(message.getEncryptionCtx().isEmpty());
            assertEquals("plain", new String(message.getData(), StandardCharsets.UTF_8));
        }
    }

    private static KeyPair keyPair() throws Exception {
        var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        return generator.generateKeyPair();
    }

    private static String pem(String type, byte[] key) {
        return "-----BEGIN " + type + "-----\n" + Base64.getMimeEncoder(64, new byte[] { '\n' }).encodeToString(key) + "\n-----END " + type + "-----";
    }

    private static String dataUrl(String pem) {
        return "data:application/x-pem-file;base64," + Base64.getEncoder().encodeToString(pem.getBytes(StandardCharsets.UTF_8));
    }
}
