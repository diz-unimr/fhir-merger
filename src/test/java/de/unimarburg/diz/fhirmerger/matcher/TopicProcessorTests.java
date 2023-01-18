package de.unimarburg.diz.fhirmerger.matcher;

import static org.assertj.core.api.Assertions.assertThat;

import de.unimarburg.diz.fhirmerger.TopicProcessor;
import java.time.Instant;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.api.Record;
import org.hl7.fhir.r4.model.Bundle;
import org.junit.jupiter.api.Test;

public class TopicProcessorTests {

    @Test
    void process_SetsTopicPrefix() {

        var record = new Record<>("key", new Bundle(), Instant
            .now()
            .toEpochMilli());

        var processor = new TopicProcessor(new BaseMatcher() {
            @Override
            public boolean supports(String topic) {
                return true;
            }

            @Override
            public Record<String, Bundle> match(Record<String, Bundle> record, String topic) {
                return record;
            }
        });

        // setup context
        var context = new MockProcessorContext<String, Bundle>();
        context.setRecordMetadata("test-fhir", 0, 0);
        processor.init(context);

        // act
        processor.process(record);

        // assert
        assertThat(context.forwarded())
            .extracting(CapturedForward::record)
            .singleElement()
            .isEqualTo(record.withKey("test-key"));
    }

    @Test
    void process_DoesNotForwardRecordOnNoMatch() {

        var record = new Record<>("key", new Bundle(), Instant
            .now()
            .toEpochMilli());

        var processor = new TopicProcessor(new BaseMatcher() {
            @Override
            public boolean supports(String topic) {
                return true;
            }

            @Override
            public Record<String, Bundle> match(Record<String, Bundle> record, String topic) {
                return null;
            }
        });

        // setup context
        var context = new MockProcessorContext<String, Bundle>();
        context.setRecordMetadata("test-fhir", 0, 0);
        processor.init(context);

        // act
        processor.process(record);

        // assert
        assertThat(context.forwarded()).isEmpty();
    }

}
