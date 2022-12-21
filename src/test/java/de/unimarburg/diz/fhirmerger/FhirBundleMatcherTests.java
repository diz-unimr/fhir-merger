package de.unimarburg.diz.fhirmerger;

import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import java.util.Date;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Condition;
import org.junit.jupiter.api.Test;

class FhirBundleMatcherTests {

    @Test
    void Bundle_FiltersCondition() {

        var condition = new Condition().setRecordedDate(new Date());
        var bundle = new Bundle().addEntry(new BundleEntryComponent().setResource(condition));
        var expression = "Bundle.entry.where(resource.is(Condition) and resource.recordedDate > @2021)";

        var matcher = new FhirBundleMatcher(new FhirPathR4(FhirContext.forR4()), expression);
        var result = matcher.parse(bundle);

        assertThat(result.getEntry())
            .extracting(BundleEntryComponent::getResource)
            .containsExactly(condition);
    }
}
