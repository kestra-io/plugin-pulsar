package io.kestra.plugin.pulsar;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

final class EncryptionKey {
    private EncryptionKey() {
    }

    static String url(String key) {
        if (key.stripLeading().startsWith("-----BEGIN ")) {
            return "data:application/x-pem-file;base64," + Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
        }
        return key;
    }
}
