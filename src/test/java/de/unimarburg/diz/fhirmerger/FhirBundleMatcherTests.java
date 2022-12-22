package de.unimarburg.diz.fhirmerger;

import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import java.util.Date;
import java.util.Map;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Organization;
import org.junit.jupiter.api.Test;

class FhirBundleMatcherTests {

    @Test
    void parse_FiltersConditionModule() {

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

        var matcher = new FhirBundleMatcher(new FhirPathR4(FhirContext.forR4()),
            Map.of(inputTopic, expression));
        var result = matcher.parse(bundle);

        assertThat(result).isEqualTo(bundle);
    }
}
