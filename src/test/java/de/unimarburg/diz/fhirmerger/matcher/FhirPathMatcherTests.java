package de.unimarburg.diz.fhirmerger.matcher;

import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import de.unimarburg.diz.fhirmerger.config.MergerProperties.MatcherProperties;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.streams.processor.api.Record;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Period;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FhirPathMatcherTests {

    private static Stream<Arguments> match_FiltersEncounterAtPeriodStart() {
        return Stream.of(Arguments.of(
            "Bundle.entry.where(resource.is(Encounter) and resource.period.start < @2022-06-14)",
            true), Arguments.of(
            "Bundle.entry.where(resource.is(Encounter) and resource.period.start >= @2022-06-14)",
            false));
    }

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

    @ParameterizedTest
    @MethodSource
    void match_FiltersEncounterAtPeriodStart(String expression, boolean isMatch) {
        var inputTopic = "test";

        var encounter = new Encounter().setPeriod(new Period().setStart(Date.from(LocalDate
            .of(2021, 1, 1)
            .atStartOfDay(ZoneId.systemDefault())
            .toInstant())));

        var bundle = new Bundle();
        bundle
            .getMeta()
            .setSource(inputTopic);
        bundle
            .addEntry(new BundleEntryComponent().setResource(encounter))
            .addEntry(new BundleEntryComponent().setResource(new Organization()));

        var matcherProps = new MatcherProperties();
        matcherProps.setTopic(inputTopic);
        matcherProps.setExpression(expression);
        matcherProps.setType(FhirPathMatcher.type);

        var matcher = new FhirPathMatcher(new FhirPathR4(FhirContext.forR4()),
            Map.of(inputTopic, List.of(matcherProps)));
        var result = matcher.match(new Record<>("key", bundle, 0), inputTopic);

        assertThat(result != null).isEqualTo(isMatch);
    }

    @Test
    void match_FiltersPatientModule() {

        var inputTopic = "patient";
        var expression = "Bundle.entry.where(resource.is(Patient) and resource.meta.lastUpdated < @2022-02-19)";

        var patient = new Patient();
        patient
            .getMeta()
            .setLastUpdated(Date.from(LocalDate
                .of(2022, 2, 18)
                .atStartOfDay(ZoneId.systemDefault())
                .toInstant()));

        var bundle = new Bundle();
        bundle.addEntry(new BundleEntryComponent().setResource(patient));

        var matcherProps = new MatcherProperties();
        matcherProps.setTopic(inputTopic);
        matcherProps.setExpression(expression);
        matcherProps.setType(FhirPathMatcher.type);

        var matcher = new FhirPathMatcher(new FhirPathR4(FhirContext.forR4()),
            Map.of(inputTopic, List.of(matcherProps)));
        var result = matcher.match(new Record<>("key", bundle, 0), inputTopic);

        assertThat(result
            .value()
            .getEntryFirstRep()).isEqualTo(bundle.getEntryFirstRep());
    }
}
