package io.kestra.plugin.pulsar;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public abstract class PulsarService {
    public static String decodeBase64(RunContext runContext, String value) throws IllegalVariableEvaluationException, IOException {
        Path path = runContext.workingDir().createTempFile(Base64.getDecoder().decode(runContext.render(value).replace("\n", "")));

        return path.toAbsolutePath().toString();
    }

    public static PulsarClient client(PulsarConnectionInterface pulsarConnectionInterface, RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        ClientBuilder builder = PulsarClient.builder()
            .serviceUrl(runContext.render(pulsarConnectionInterface.getUri()).as(String.class).orElseThrow());

        if (pulsarConnectionInterface.getAuthenticationToken() != null) {
            builder.authentication(AuthenticationFactory.token(runContext.render(pulsarConnectionInterface.getAuthenticationToken()).as(String.class).orElseThrow()));
        }

        if (pulsarConnectionInterface.getTlsOptions() != null) {
            Map<String, String> authParams = new HashMap<>();

            if (pulsarConnectionInterface.getTlsOptions().getCert() != null) {
                authParams.put("tlsCertFile", PulsarService.decodeBase64(runContext, runContext.render(pulsarConnectionInterface.getTlsOptions().getCert()).as(String.class).orElseThrow()));
            }

            if (pulsarConnectionInterface.getTlsOptions().getKey() != null) {
                authParams.put("tlsKeyFile", PulsarService.decodeBase64(runContext, runContext.render(pulsarConnectionInterface.getTlsOptions().getKey()).as(String.class).orElseThrow()));
            }

            Authentication tlsAuth = AuthenticationFactory.create(AuthenticationTls.class.getName(), authParams);

            builder.authentication(tlsAuth);

            if (pulsarConnectionInterface.getTlsOptions().getCa() != null) {
                builder.tlsTrustCertsFilePath(PulsarService.decodeBase64(runContext, runContext.render(pulsarConnectionInterface.getTlsOptions().getCa()).as(String.class).orElseThrow()));
            }
        }

        return builder.build();
    }
}
