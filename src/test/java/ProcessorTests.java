import de.unimarburg.diz.fhirmerger.Processor;
import de.unimarburg.diz.fhirmerger.config.MergerProperties;
import de.unimarburg.diz.fhirmerger.config.MergerProperties.MatcherProperties;
import de.unimarburg.diz.fhirmerger.matcher.BaseMatcher;
import de.unimarburg.diz.fhirmerger.serde.FhirDeserializer;
import de.unimarburg.diz.fhirmerger.serde.FhirSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.test.TestRecord;
import org.hl7.fhir.r4.model.Bundle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessorTests {

    @Test
    public void buildPipelineConfiguresProcessor() {

        var topicName = "test-fhir";
        var outputTopicName = "merged";

        var matcherProps = new MatcherProperties();
        matcherProps.setTopic(topicName);
        matcherProps.setExpression("Bundle.where(true)");
        matcherProps.setType("fhir");

        var props = new MergerProperties();
        props.setInput(List.of(matcherProps));
        props
            .getOutput()
            .setTopic(outputTopicName);

        var matcher = new TestMatcher();

        // build stream
        var builder = new StreamsBuilder();
        new Processor(props, matcher).buildPipeline(builder);

        try (var driver = new TopologyTestDriver(builder.build())) {

            var testTopic =
                driver.createInputTopic(topicName, new StringSerializer(),
                    new FhirSerializer<>());
            var outputTopic = driver.createOutputTopic(outputTopicName,
                new StringDeserializer(),
                new FhirDeserializer<>(Bundle.class));

            var bundle = new Bundle();
            bundle
                .getMeta()
                .setSource(topicName);

            testTopic.pipeInput("42", bundle);

            // get record from output topic
            var outputRecords = outputTopic.readRecordsToList();

            // assert
            assertThat(outputRecords)
                .singleElement()
                .extracting(TestRecord::key, x -> x
                    .value()
                    .getMeta()
                    .getSource())
                .containsExactly("test-42", bundle
                    .getMeta()
                    .getSource());

        }
    }

    static class TestMatcher extends BaseMatcher {


        @Override
        public Record<String, Bundle> match(Record<String, Bundle> record,
                                            String topic) {
            return null;
        }

        @Override
        public boolean supports(String topic) {
            return false;
        }
    }

}
