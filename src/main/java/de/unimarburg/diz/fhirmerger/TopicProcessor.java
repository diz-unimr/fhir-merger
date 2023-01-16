package de.unimarburg.diz.fhirmerger;

import de.unimarburg.diz.fhirmerger.matcher.BaseMatcher;
import de.unimarburg.diz.fhirmerger.matcher.MatchProcessor;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.hl7.fhir.r4.model.Bundle;

public class TopicProcessor extends ContextualProcessor<String, Bundle, String, Bundle> {

    private final List<BaseMatcher> processors;

    public TopicProcessor(BaseMatcher... processors) {
        this.processors = Arrays.asList(processors);
    }

    @Override
    public void process(Record<String, Bundle> record) {
        // get source topic
        var topic = context()
            .recordMetadata()
            .orElseThrow()
            .topic();

        var pipeline = processors
            .stream()
            .filter(m -> m.supports(topic))
            .map(p -> (MatchProcessor) p)
            .reduce(MatchProcessor::chain)
            .orElse(MatchProcessor.identity());

        var matched = pipeline.match(record, topic);

        if (matched == null) {
            return;
        }

        // set key prefix from source topic
        var topicPrefix = StringUtils.substringBefore(topic, "-");

        context().forward(
            new Record<>(String.format("%s-%s", topicPrefix, record.key()), record.value(),
                record.timestamp(), record.headers()));
    }
}
