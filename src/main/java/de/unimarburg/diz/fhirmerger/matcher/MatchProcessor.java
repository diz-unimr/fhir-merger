package de.unimarburg.diz.fhirmerger.matcher;

import java.util.Objects;
import org.apache.kafka.streams.processor.api.Record;
import org.hl7.fhir.r4.model.Bundle;
import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface MatchProcessor {

    static MatchProcessor identity() {
        return (t, c) -> t;
    }

    @NotNull
    default MatchProcessor chain(@NotNull MatchProcessor after) {
        Objects.requireNonNull(after);
        return (record, topic) -> after.match(this.match(record, topic), topic);
    }

    Record<String, Bundle> match(Record<String, Bundle> record, String topic);

}
