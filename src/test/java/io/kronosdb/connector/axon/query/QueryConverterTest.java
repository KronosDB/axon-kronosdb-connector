package io.kronosdb.connector.axon.query;

import io.kronosdb.protocol.MetadataValue;
import io.kronosdb.protocol.ProcessingInstruction;
import io.kronosdb.protocol.ProcessingKey;
import io.kronosdb.protocol.Query;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class QueryConverterTest {

    private static final QueryMessage QUERY =
            new GenericQueryMessage(new MessageType("FindOrders", "1"), "{}".getBytes(StandardCharsets.UTF_8));

    private static Optional<ProcessingInstruction> timeout(Query query) {
        return query.instructions().stream().filter(i -> i.key().equals(ProcessingKey.TIMEOUT)).findFirst();
    }

    @Test
    void aQuerySendsNoTimeoutSoTheServersApplies() {
        assertThat(timeout(QueryConverter.convertQueryMessage(QUERY, "client", "component"))).isEmpty();
    }

    @Test
    void aConfiguredQueryTimeoutIsSent() {
        Query query = QueryConverter.convertQueryMessage(QUERY, "client", "component", 90_000);
        assertThat(timeout(query)).hasValue(
                new ProcessingInstruction(ProcessingKey.TIMEOUT, new MetadataValue.OfNumber(90_000)));
    }
}
