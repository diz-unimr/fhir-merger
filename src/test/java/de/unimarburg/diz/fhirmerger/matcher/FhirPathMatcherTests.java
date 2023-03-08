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
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Period;
import org.hl7.fhir.r4.model.Procedure;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class FhirPathMatcherTests {

    private static Stream<Arguments> match_FiltersEncounterAtPeriodStart() {
        return Stream.of(Arguments.of(
            "Bundle.entry.where(resource.is(Encounter) and resource.period.start < @2022-06-14)",
            true), Arguments.of(
            "Bundle.entry.where(resource.is(Encounter) and resource.period.start >= @2022-06-14)",
            false));
    }

    @ParameterizedTest()
    @CsvSource({"2023-03-08,true", "2017-04-01,false"})
    void match_FiltersConditionModule(String recorded, boolean matches) {

        var inputTopic = "bar";
        // pick whole bundle
        var expression = "Bundle.where(entry.where(resource.is(Condition) and resource.recordedDate >= @2021-04-15))";

        var recordedDate = Date.from(LocalDate
            .parse(recorded)
            .atStartOfDay(ZoneId.of("Europe/Berlin"))
            .toInstant());
        var condition = new Condition().setRecordedDate(recordedDate);
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

        if (matches) {
            assertThat(result.value()).isEqualTo(bundle);
        } else {
            assertThat(result).isNull();
        }

    }

    @ParameterizedTest()
    @CsvSource({"2023-03-08,true", "2017-04-01,false"})
    void match_FiltersProcedureModule(String recorded, boolean matches) {

        var inputTopic = "bar";
        // pick whole bundle
        var expression = "Bundle.where(entry.where(resource.is(Procedure) and resource.performed >= @2021-04-15))";

        var performed = Date.from(LocalDate
            .parse(recorded)
            .atStartOfDay(ZoneId.of("Europe/Berlin"))
            .toInstant());
        var procedure = new Procedure().setPerformed(new DateTimeType(performed));

        var bundle = new Bundle();
        bundle
            .getMeta()
            .setSource(inputTopic);
        bundle
            .addEntry(new BundleEntryComponent().setResource(procedure))
            .addEntry(new BundleEntryComponent().setResource(new Organization()));

        var matcherProps = new MatcherProperties();
        matcherProps.setTopic(inputTopic);
        matcherProps.setExpression(expression);
        matcherProps.setType(FhirPathMatcher.type);

        var matcher = new FhirPathMatcher(new FhirPathR4(FhirContext.forR4()),
            Map.of(inputTopic, List.of(matcherProps)));
        var result = matcher.match(new Record<>("key", bundle, 0), inputTopic);

        if (matches) {
            assertThat(result.value()).isEqualTo(bundle);
        } else {
            assertThat(result).isNull();
        }
    }


    @ParameterizedTest()
    @CsvSource({"2017-04-01,true", "2023-03-08,false"})
    void match_FiltersSingleProcedures(String recorded, boolean matches) {

        var inputTopic = "bar";
        // pick single Procedure
        var expression = "Bundle.entry.where(resource.is(Procedure) and resource.performed < @2021-04-15)";

        var performed = Date.from(LocalDate
            .parse(recorded)
            .atStartOfDay(ZoneId.of("Europe/Berlin"))
            .toInstant());
        var procedure = new Procedure().setPerformed(new DateTimeType(performed));

        var bundle = new Bundle();
        bundle
            .getMeta()
            .setSource(inputTopic);
        bundle
            .addEntry(new BundleEntryComponent().setResource(procedure))
            .addEntry(new BundleEntryComponent().setResource(new Organization()));

        var matcherProps = new MatcherProperties();
        matcherProps.setTopic(inputTopic);
        matcherProps.setExpression(expression);
        matcherProps.setType(FhirPathMatcher.type);

        var matcher = new FhirPathMatcher(new FhirPathR4(FhirContext.forR4()),
            Map.of(inputTopic, List.of(matcherProps)));
        var result = matcher.match(new Record<>("key", bundle, 0), inputTopic);

        if (matches) {
            // new bundle created
            assertThat(result.value()).isNotEqualTo(bundle);
            // only the match is included
            assertThat(result
                .value()
                .getEntry())
                .extracting(BundleEntryComponent::getResource)
                .containsOnly(procedure);
        } else {
            assertThat(result).isNull();
        }
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
