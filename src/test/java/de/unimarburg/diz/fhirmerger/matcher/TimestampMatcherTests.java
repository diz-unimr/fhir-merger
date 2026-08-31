package de.unimarburg.diz.fhirmerger.matcher;

import de.unimarburg.diz.fhirmerger.config.MergerProperties.MatcherProperties;
import org.apache.kafka.streams.processor.api.Record;
import org.hl7.fhir.r4.model.Bundle;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TimestampMatcherTests {


    @Test
    void matchTokenizesString() {
        var props = new MatcherProperties();
        props.setType(TimestampMatcher.TYPE);
        props.setTopic("topic");
        props.setExpression(">= 2023-01-01");
        var matcher = new TimestampMatcher(List.of(props));

        var bundle = new Bundle();

        var record = new Record<>("test", bundle, Instant
            .now()
            .toEpochMilli());
        var result = matcher.match(record, "topic");

        assertThat(result).isEqualTo(record);
    }

}
