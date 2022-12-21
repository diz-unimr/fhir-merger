package de.unimarburg.diz.fhirmerger;

import de.unimarburg.diz.fhirmerger.serde.FhirSerde;
import java.util.Collection;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.hl7.fhir.r4.model.Bundle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

@Service
public class Processor {

    private final Collection<String> inputTopics;
    private final String outputTopic;

    private final FhirBundleMatcher matcher;

    public Processor(@Value(value = "${spring.kafka.input-topics}") Collection<String> inputTopics,
        @Value(value = "${spring.kafka.output-topic}") String outputTopic,
        FhirBundleMatcher matcher) {
        Assert.notEmpty(inputTopics, "'spring.kafka.input-topics' must be set");
        Assert.hasLength(outputTopic, "'spring.kafka.output-topic' must be set");
        this.inputTopics = inputTopics;
        this.outputTopic = outputTopic;
        this.matcher = matcher;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {

        var stream = streamsBuilder.stream(inputTopics,
            Consumed.with(new StringSerde(), new FhirSerde<>(Bundle.class)));
        stream
            .process(TopicKeyProcessor::new)
            .mapValues(matcher::parse)
            .filter((k, v) -> v != null)
            .to(outputTopic, Produced.with(new StringSerde(), new FhirSerde<>(Bundle.class)));
    }
}
