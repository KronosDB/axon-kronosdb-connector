package io.kronosdb.connector.axon.event;

import io.kronosdb.protocol.Condition;
import io.kronosdb.protocol.Criterion;
import io.kronosdb.protocol.Tag;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventstreaming.EventCriterion;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Converts Axon Framework event conditions (append, sourcing, streaming) into
 * native protocol values.
 */
public final class ConditionConverter {

    private ConditionConverter() {
    }

    /**
     * Converts an {@link AppendCondition} to a protocol {@link Condition}, or {@code null} when it has
     * no criteria: an append that conflicts with nothing needs no check.
     */
    public static @Nullable Condition convertAppendCondition(AppendCondition condition) {
        List<Criterion> criteria = convertEventCriteria(condition.criteria().flatten());
        if (criteria.isEmpty()) {
            return null;
        }
        long marker = GlobalIndexConsistencyMarker.position(condition.consistencyMarker());
        if (marker < 0) {
            marker = 0;
        }
        return new Condition(marker, criteria);
    }

    /**
     * The position a {@link SourcingCondition} reads from.
     */
    public static long sourcingFrom(SourcingCondition condition) {
        long fromIndex = GlobalIndexPosition.toIndex(condition.start());
        // GlobalIndexPosition.toIndex returns Long.MIN_VALUE for Position.START.
        // KronosDB uses 0 to mean "start from the beginning".
        return fromIndex < 0 ? 0 : fromIndex;
    }

    /**
     * The criteria a {@link SourcingCondition} filters by.
     */
    public static List<Criterion> sourcingCriteria(SourcingCondition condition) {
        return convertEventCriteria(condition.criteria().flatten());
    }

    /**
     * Extracts the starting sequence position from a {@link StreamingCondition}.
     */
    public static long extractStreamFromSequence(StreamingCondition condition) {
        return condition.position().position().orElse(0);
    }

    /**
     * Extracts the criteria from a {@link StreamingCondition}.
     */
    public static List<Criterion> extractStreamCriteria(StreamingCondition condition) {
        return convertEventCriteria(condition.criteria().flatten());
    }

    private static List<Criterion> convertEventCriteria(Set<EventCriterion> eventCriteria) {
        List<Criterion> criteria = new ArrayList<>(eventCriteria.size());
        for (EventCriterion criterion : eventCriteria) {
            criteria.add(convertEventCriterion(criterion));
        }
        return criteria;
    }

    private static Criterion convertEventCriterion(EventCriterion eventCriterion) {
        List<String> names = new ArrayList<>();
        for (QualifiedName type : eventCriterion.types()) {
            names.add(type.name());
        }
        List<Tag> tags = TaggedEventConverter.convertTagSet(eventCriterion.tags());
        return new Criterion(names, tags);
    }

    static String describe(Criterion criterion) {
        List<String> tags = new ArrayList<>();
        for (Tag tag : criterion.tags()) {
            tags.add(utf8(tag.key()) + "=" + utf8(tag.value()));
        }
        return "names=" + criterion.names() + " tags=" + tags;
    }

    static String describeTag(Tag tag) {
        return utf8(tag.key()) + "=" + utf8(tag.value());
    }

    private static String utf8(ByteBuffer buffer) {
        return StandardCharsets.UTF_8.decode(buffer.duplicate()).toString();
    }
}
