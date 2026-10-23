package io.kestra.plugin.pulsar;

import java.nio.charset.StandardCharsets;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ReaderTest {
    @Test
    void parsesTextualMessageIds() throws Exception {
        assertEquals(new MessageIdImpl(12, 0, -1), Reader.parseMessageId("12:0:-1"));
        assertEquals(new MessageIdImpl(12, 3, -1), Reader.parseMessageId("12:3"));
        assertEquals(new MessageIdImpl(12, 3, 2), Reader.parseMessageId("12:3:2"));
    }

    @Test
    void parsesSentinelMessageIds() throws Exception {
        assertSame(MessageId.earliest, Reader.parseMessageId("earliest"));
        assertSame(MessageId.latest, Reader.parseMessageId("latest"));
    }

    @Test
    void keepsParsingTheExistingByteForm() throws Exception {
        MessageId messageId = new MessageIdImpl(12, 0, -1);
        String byteForm = new String(messageId.toByteArray(), StandardCharsets.UTF_8);

        assertEquals(messageId, Reader.parseMessageId(byteForm));
    }
}
