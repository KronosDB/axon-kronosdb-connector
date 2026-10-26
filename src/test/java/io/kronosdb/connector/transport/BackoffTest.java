package io.kronosdb.connector.transport;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffTest {

    @Test
    void delaysGrowToACapWithJitterOnTop() {
        assertThat(Backoff.delay(1)).isBetween(500L, 625L);
        assertThat(Backoff.delay(3)).isBetween(2000L, 2500L);
        assertThat(Backoff.delay(40)).isBetween(30_000L, 37_500L);
    }

    @Test
    void streamsThatKeepEndingSoonAfterTheyOpenStartTheirNextReconnectLater() {
        Backoff.Flaps flaps = new Backoff.Flaps();
        assertThat(flaps.startAttempt()).isZero();

        flaps.opened();
        assertThat(flaps.ended()).isEqualTo(1);
        assertThat(flaps.startAttempt()).isZero();

        flaps.opened();
        assertThat(flaps.ended()).isEqualTo(2);
        assertThat(flaps.startAttempt()).isEqualTo(1);

        flaps.opened();
        assertThat(flaps.ended()).isEqualTo(3);
        assertThat(flaps.startAttempt()).isEqualTo(2);
    }
}
