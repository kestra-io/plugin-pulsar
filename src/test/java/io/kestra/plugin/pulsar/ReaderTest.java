package io.kestra.plugin.pulsar;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReaderTest {
    @Test
    void parsesTextualMessageIds() {
        assertThat(Reader.parseMessageId("12:3"), is(new MessageIdImpl(12, 3, -1)));
        assertThat(Reader.parseMessageId("12:3:2"), is(new MessageIdImpl(12, 3, 2)));
        assertThat(Reader.parseMessageId("200:300:-1"), is(new MessageIdImpl(200, 300, -1)));
        assertThat(Reader.parseMessageId("12:3:-1:5"), is(new BatchMessageIdImpl(12, 3, -1, 5)));
    }

    @Test
    void roundTripsToString() {
        var batch = new BatchMessageIdImpl(12, 3, 1, 5);
        assertThat(Reader.parseMessageId(batch.toString()), is(batch));
    }

    @Test
    void parsesSentinelMessageIds() {
        assertThat(Reader.parseMessageId("earliest"), sameInstance(MessageId.earliest));
        assertThat(Reader.parseMessageId("latest"), sameInstance(MessageId.latest));
    }

    @Test
    void rejectsInvalidMessageIds() {
        for (var invalid : new String[]{"foo", "12", "12:3:", "12:3:abc", "1:2:3:4:5"}) {
            var e = assertThrows(IllegalArgumentException.class, () -> Reader.parseMessageId(invalid));
            assertThat(e.getMessage(), containsString("expected 'ledger:entry[:partition[:batchIndex]]'"));
        }
    }
}
