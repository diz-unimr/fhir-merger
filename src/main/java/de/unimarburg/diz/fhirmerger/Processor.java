package de.unimarburg.diz.fhirmerger;

import de.unimarburg.diz.fhirmerger.config.MergerProperties;
import de.unimarburg.diz.fhirmerger.config.MergerProperties.TopicMatcher;
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

    private final FhirBundleMatcher matcher;

    public Processor(MergerProperties properties, FhirBundleMatcher matcher) {
        this.properties = properties;
        this.matcher = matcher;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {

        var stream = streamsBuilder.stream(properties
            .getInput()
            .stream()
            .map(TopicMatcher::getTopic)
            .toList(), Consumed.with(new StringSerde(), new FhirSerde<>(Bundle.class)));
        stream
            .process(TopicKeyProcessor::new)
            .mapValues(matcher::parse)
            .filter((k, v) -> v != null)
            .to(properties
                .getOutput()
                .getTopic(), Produced.with(new StringSerde(), new FhirSerde<>(Bundle.class)));
    }
}
