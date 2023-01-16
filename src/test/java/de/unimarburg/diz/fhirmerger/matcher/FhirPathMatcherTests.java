package de.unimarburg.diz.fhirmerger.matcher;

import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import de.unimarburg.diz.fhirmerger.config.MergerProperties.MatcherProperties;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.processor.api.Record;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Organization;
import org.junit.jupiter.api.Test;

class FhirPathMatcherTests {

    @Test
    void match_FiltersConditionModule() {

        var inputTopic = "bar";
        // pick whole bundle
        var expression = "Bundle.where((entry.resource.is(Condition) and entry.resource.recordedDate > @2021).exists())";

        var condition = new Condition().setRecordedDate(new Date());
        var bundle = new Bundle();
        bundle
            .getMeta()
            .setSource(inputTopic);
        bundle
            .addEntry(new BundleEntryComponent().setResource(condition))
            .addEntry(new BundleEntryComponent().setResource(new Organization()));

        var matcherProps = new MatcherProperties();
        matcherProps.setTopic(inputTopic);
        matcherProps.setExpression(expression);
        matcherProps.setType(FhirPathMatcher.type);

        var matcher = new FhirPathMatcher(new FhirPathR4(FhirContext.forR4()),
            Map.of(inputTopic, List.of(matcherProps)));
        var result = matcher.match(new Record<>("key", bundle, 0), inputTopic);

        assertThat(result.value()).isEqualTo(bundle);
    }

    @Test
    void match_FiltersProcedureModule() {

        var inputTopic = "bar";
        // pick whole bundle
        var expression = "Bundle.where((entry.resource.is(Condition) and entry.resource.recordedDate > @2021).exists())";

        var condition = new Condition().setRecordedDate(new Date());
        var bundle = new Bundle();
        bundle
            .getMeta()
            .setSource(inputTopic);
        bundle
            .addEntry(new BundleEntryComponent().setResource(condition))
            .addEntry(new BundleEntryComponent().setResource(new Organization()));

        var matcherProps = new MatcherProperties();
        matcherProps.setTopic(inputTopic);
        matcherProps.setExpression(expression);
        matcherProps.setType(FhirPathMatcher.type);

        var matcher = new FhirPathMatcher(new FhirPathR4(FhirContext.forR4()),
            Map.of(inputTopic, List.of(matcherProps)));
        var result = matcher.match(new Record<>("key", bundle, 0), inputTopic);

        assertThat(result.value()).isEqualTo(bundle);
    }
}
