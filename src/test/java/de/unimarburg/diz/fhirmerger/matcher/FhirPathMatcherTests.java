package de.unimarburg.diz.fhirmerger.matcher;

import ca.uhn.fhir.context.FhirContext;
import de.unimarburg.diz.fhirmerger.config.MergerProperties.MatcherProperties;
import org.apache.kafka.streams.processor.api.Record;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class FhirPathMatcherTests {

    private static Stream<Arguments> match_FiltersEncounterAtPeriodStart() {
        return Stream.of(Arguments.of(
                "Bundle.entry.where(resource.is(Encounter) and resource.period.start < @2022-06-14)",
                true), Arguments.of(
                "Bundle.entry.where(resource.is(Encounter) and resource.period.start >= @2022-06-14)",
                false));
    }

    public static Parameters GET_DUMMY_PATIENT_MERGE() {
        return new Parameters().addParameter(new ParametersParameterComponent()
                .setName("operation")
                .addPart(new ParametersParameterComponent()
                        .setName("type")
                        .setValue(new CodeType("add")))
                .addPart(new ParametersParameterComponent()
                        .setName("path")
                        .setValue(new CodeType("Patient")))
                .addPart(new ParametersParameterComponent()
                        .setName("name")
                        .setValue(new CodeType("link")))
                .addPart(new ParametersParameterComponent()
                        .setName("value")
                        .addPart(new ParametersParameterComponent()
                                .setName("other")
                                .setValue(new Reference()
                                        .setReference(
                                                "Patient?identifier=https://fhir.diz.uni-marburg.de/sid/patient-id|000002")
                                        .setType("Patient")))
                        .addPart(new ParametersParameterComponent()
                                .setName("type")
                                .setValue(new CodeType("replaced-by")))));
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
        matcherProps.setType(FhirPathMatcher.TYPE);

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
        matcherProps.setType(FhirPathMatcher.TYPE);

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
        matcherProps.setType(FhirPathMatcher.TYPE);

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
        matcherProps.setType(FhirPathMatcher.TYPE);

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
        matcherProps.setType(FhirPathMatcher.TYPE);

        var matcher = new FhirPathMatcher(new FhirPathR4(FhirContext.forR4()),
                Map.of(inputTopic, List.of(matcherProps)));
        var result = matcher.match(new Record<>("key", bundle, 0), inputTopic);

        assertThat(result
                .value()
                .getEntryFirstRep()).isEqualTo(bundle.getEntryFirstRep());
    }

    @Test
    void match_FiltersPatientModuleWithPatientPatch() {

        var inputTopic = "patient";
        var expression = "Bundle.entry.where(resource.is(Patient) or resource.is(Observation) "
                + "or (resource.is(Parameters) and resource.parameter.part.where(name = 'path' and value = 'Patient').exists()))";

        var patientPatch = GET_DUMMY_PATIENT_MERGE();

        var bundle = new Bundle();
        bundle.addEntry(new BundleEntryComponent().setResource(patientPatch));
        bundle.addEntry(new BundleEntryComponent().setResource(new Encounter()));

        var matcherProps = new MatcherProperties();
        matcherProps.setTopic(inputTopic);
        matcherProps.setExpression(expression);
        matcherProps.setType(FhirPathMatcher.TYPE);

        var matcher = new FhirPathMatcher(new FhirPathR4(FhirContext.forR4()),
                Map.of(inputTopic, List.of(matcherProps)));
        var result = matcher.match(new Record<>("key", bundle, 0), inputTopic);

        assertThat(result
                .value()
                .getEntryFirstRep()).isEqualTo(bundle.getEntryFirstRep());
        assertThat(result
                .value()
                .getEntry()
                .size())
                .as("unfitting bundle entries are removed.")
                .isEqualTo(1);
    }

    @Test
    void match_FiltersPatientModulePatientResource() {

        var inputTopic = "patient";
        var expression = """
                Bundle.entry.where(resource.is(Patient) or resource.is(Observation) or
                (resource.is(Parameters) and (resource.parameter.part.where(name = 'path' and value = 'Patient')).exists()))""";

        var patientPatch = new Patient();

        var bundle = new Bundle();
        bundle.addEntry(new BundleEntryComponent().setResource(patientPatch));

        var matcherProps = new MatcherProperties();
        matcherProps.setTopic(inputTopic);
        matcherProps.setExpression(expression);
        matcherProps.setType(FhirPathMatcher.TYPE);

        var matcher = new FhirPathMatcher(new FhirPathR4(FhirContext.forR4()),
                Map.of(inputTopic, List.of(matcherProps)));
        var result = matcher.match(new Record<>("key", bundle, 0), inputTopic);

        assertThat(result
                .value()
                .getEntryFirstRep()).isEqualTo(bundle.getEntryFirstRep());
    }

    /**
     * <p>Bundle.entry.where(resource.is(Encounter) and (resource.class.code != 'IMP' or
     * resource.period.start >= @2022-06-14) or resource.is(Location) or
     * resource.is(Organization))</p>
     * <br/>We accept encounter class IMP only if it is after '2022-06-14' or it must be another
     * encounter class. Location and Organization resources are always accepted.
     * <li>
     * IMP class encounter before that date are provided via p21</li>
     * <li> other class encounter may override encounter
     * provided by visit-to-fhir encounter since existing one have only minimal content.</li>
     * <li> we always accept location and organization resources since they never hurt :)</li>
     */
    @ParameterizedTest
    @ValueSource(strings = {"IMP", "AMB", "PRENC"})
    void match_FiltersOldEncounterNotP21(String encClass) {

        var inputTopic = "adt";

        final String isNotInpatientEncounterOrWithinDateRange = "resource.is(Encounter) and (resource.class.code != 'IMP' or resource.period.start >= @2022-06-14)";
        final String isLocationOrOrganization = "resource.is(Location) or resource.is(Organization)";
        var expression = String.format("Bundle.entry.where(%s or %s)",
                isNotInpatientEncounterOrWithinDateRange, isLocationOrOrganization);

        var patient = new Patient();
        var enc = new Encounter().setPeriod(new Period().setStart(Date.from(LocalDate
                .of(2021, 7, 18)
                .atStartOfDay(ZoneId.systemDefault())
                .toInstant()))).setClass_(new Coding().setCode(encClass));

        var org = new Organization().setName("Org1");
        var loc = new Location().setName("her I am");

        var bundle = new Bundle();

        bundle.addEntry(new BundleEntryComponent().setResource(patient));
        bundle.addEntry(new BundleEntryComponent().setResource(enc));
        bundle.addEntry(new BundleEntryComponent().setResource(org));
        bundle.addEntry(new BundleEntryComponent().setResource(loc));

        var matcherProps = new MatcherProperties();
        matcherProps.setTopic(inputTopic);
        matcherProps.setExpression(expression);
        matcherProps.setType(FhirPathMatcher.TYPE);

        var matcher = new FhirPathMatcher(new FhirPathR4(FhirContext.forR4()),
                Map.of(inputTopic, List.of(matcherProps)));
        var result = matcher.match(new Record<>("key", bundle, 0), inputTopic);

        if (encClass.equals("IMP")) {
            assertThat(result.value().getEntry()).as("we expect encounter resource to be filtered")
                    .noneSatisfy(a -> assertThat(
                            a.getResource().fhirType().equals("Encounter")).isTrue());

            assertThat(result.value().getEntry()).as("one Location is expected")
                    .anySatisfy(a -> assertThat(
                            a.getResource().fhirType()
                                    .equals("Location")).isTrue());
            assertThat(result.value().getEntry()).as("one Organization is expected")
                    .anySatisfy(a -> assertThat(
                            a.getResource().fhirType().equals("Organization")).isTrue());
        } else {
            assertThat(result.value().getEntry()).as("one Encounter is expected")
                    .anySatisfy(a -> assertThat(
                            a.getResource().fhirType().equals("Encounter")).isTrue());
            assertThat(result.value().getEntry()).as("one Location is expected")
                    .anySatisfy(a -> assertThat(
                            a.getResource().fhirType()
                                    .equals("Location")).isTrue());
            assertThat(result.value().getEntry()).as("one Organization is expected")
                    .anySatisfy(a -> assertThat(
                            a.getResource().fhirType().equals("Organization")).isTrue());
        }
    }


    @Test
    void match_CreatesTransactionBundleForResources() {

        var inputTopic = "patient";
        // match by entry (resource)
        var expression = "Bundle.entry.where(resource.is(Patient))";

        var patientPatch = new Patient();

        var bundle = new Bundle();
        bundle.addEntry(new BundleEntryComponent().setResource(patientPatch));

        var matcherProps = new MatcherProperties();
        matcherProps.setTopic(inputTopic);
        matcherProps.setExpression(expression);
        matcherProps.setType(FhirPathMatcher.TYPE);

        var matcher = new FhirPathMatcher(new FhirPathR4(FhirContext.forR4()),
                Map.of(inputTopic, List.of(matcherProps)));
        var result = matcher.match(new Record<>("key", bundle, 0), inputTopic);

        assertThat(result.value().getType()).isEqualTo(Bundle.BundleType.TRANSACTION);
    }
}
