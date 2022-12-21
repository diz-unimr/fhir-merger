package de.unimarburg.diz.fhirmerger;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.hl7.fhir.r4.model.Bundle;

public class TopicKeyProcessor extends ContextualProcessor<String, Bundle, String, Bundle> {

    @Override
    public void process(Record<String, Bundle> record) {
        var topic = context()
            .recordMetadata()
            .orElseThrow()
            .topic();

        // strip meta data
        var topicPrefix = StringUtils.substringBefore(topic, "-");
        context().forward(record.withKey(String.format("%s-%s", topicPrefix, record.key())));
    }
}
