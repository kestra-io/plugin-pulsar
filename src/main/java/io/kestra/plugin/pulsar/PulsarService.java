package io.kestra.plugin.pulsar;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public abstract class PulsarService {
    public static String decodeBase64(RunContext runContext, String value) throws IllegalVariableEvaluationException, IOException {
        Path path = runContext.tempFile(Base64.getDecoder().decode(runContext.render(value).replace("\n", "")));

        return path.toAbsolutePath().toString();
    }

    public static PulsarClient client(PulsarConnectionInterface pulsarConnectionInterface, RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        ClientBuilder builder = PulsarClient.builder()
            .serviceUrl(runContext.render(pulsarConnectionInterface.getUri()));
	if (pulsarConnectionInterface.getAuthenticationToken() != null) {
	    builder.authentication(AuthenticationFactory.token(pulsarConnectionInterface.getAuthenticationToken()));
	}
        if (pulsarConnectionInterface.getTlsOptions() != null) {
            Map<String, String> authParams = new HashMap<>();

            if (pulsarConnectionInterface.getTlsOptions().getCert() != null) {
                authParams.put("tlsCertFile", PulsarService.decodeBase64(runContext, pulsarConnectionInterface.getTlsOptions().getCert()));
            }

            if (pulsarConnectionInterface.getTlsOptions().getKey() != null) {
                authParams.put("tlsKeyFile", PulsarService.decodeBase64(runContext, pulsarConnectionInterface.getTlsOptions().getKey()));
            }

            Authentication tlsAuth = AuthenticationFactory.create(AuthenticationTls.class.getName(), authParams);

            builder.authentication(tlsAuth);

            if (pulsarConnectionInterface.getTlsOptions().getCa() != null) {
                builder.tlsTrustCertsFilePath(PulsarService.decodeBase64(runContext, pulsarConnectionInterface.getTlsOptions().getCa()));
            }
        }

        return builder.build();
    }
}
