package de.unimarburg.diz.fhirmerger;

import java.util.List;
import java.util.Map;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class FhirBundleMatcher {

    private final FhirPathR4 engine;
    private final Map<String, String> expressions;

    @Autowired
    public FhirBundleMatcher(FhirPathR4 engine,
        @Qualifier("matchExpressions") Map<String, String> expressions) {
        this.engine = engine;
        this.expressions = expressions;
    }

    Bundle parse(Bundle bundle) {
        var matches = match(bundle);

        if (matches.isEmpty()) {
            return null;
        }

        if (matches.size() == 1 && matches.get(0) instanceof Bundle b) {
            return b;
        }

        var result = new Bundle();
        result.setMeta(bundle.getMeta());
        return result.setEntry(matches
            .stream()
            .map(BundleEntryComponent.class::cast)
            .toList());

    }

    private List<IBase> match(Bundle bundle) {
        // extract source topic
        var source = bundle
            .getMeta()
            .getSource();
        // lookup match expression
        var expr = expressions.get(source);

        return engine.evaluate(bundle, expr, IBase.class);
    }
}
