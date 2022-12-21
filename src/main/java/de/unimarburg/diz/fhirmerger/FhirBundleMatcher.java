package de.unimarburg.diz.fhirmerger;

import java.util.List;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class FhirBundleMatcher {

    private final FhirPathR4 engine;
    private final String expression;

    @Autowired
    public FhirBundleMatcher(FhirPathR4 engine,
        @Value(value = "${fhir.match-expression}") String expression) {
        this.engine = engine;
        this.expression = expression;
    }

    Bundle parse(Bundle bundle) {
        var matches = match(bundle);

        if (matches.isEmpty()) {
            return null;
        }

        if (matches.size() == 1 && matches.get(0) instanceof Bundle b) {
            return b;
        }

        return new Bundle().setEntry(matches
            .stream()
            .map(BundleEntryComponent.class::cast)
            .toList());

    }

    private List<IBase> match(IBaseResource resource) {
        return engine.evaluate(resource, expression, IBase.class);
    }
}
