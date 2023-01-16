package de.unimarburg.diz.fhirmerger;

import de.unimarburg.diz.fhirmerger.config.MergerProperties;
import de.unimarburg.diz.fhirmerger.config.MergerProperties.MatcherProperties;
import de.unimarburg.diz.fhirmerger.matcher.BaseMatcher;
import de.unimarburg.diz.fhirmerger.serde.FhirSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.hl7.fhir.r4.model.Bundle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class Processor {

    private final MergerProperties properties;

    private final BaseMatcher[] matchers;

    public Processor(MergerProperties properties, BaseMatcher... matchers) {
        this.properties = properties;
        this.matchers = matchers;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {

        var stream = streamsBuilder.stream(properties
            .getInput()
            .stream()
            .map(MatcherProperties::getTopic)
            .toList(), Consumed.with(new StringSerde(), new FhirSerde<>(Bundle.class)));
        stream
            .process(() -> new TopicProcessor(matchers))
            .to(properties
                .getOutput()
                .getTopic(), Produced.with(new StringSerde(), new FhirSerde<>(Bundle.class)));
    }
}
