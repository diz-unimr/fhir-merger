package de.unimarburg.diz.fhirmerger.matcher;

import de.unimarburg.diz.fhirmerger.config.ConditionalOnMatcher;
import de.unimarburg.diz.fhirmerger.config.MergerProperties.MatcherProperties;
import org.apache.kafka.streams.processor.api.Record;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@ConditionalOnMatcher(FhirPathMatcher.type)
public class FhirPathMatcher extends BaseMatcher {

    static final String type = "fhir";
    private final FhirPathR4 engine;
    private final Map<String, String> matchers;

    @Autowired
    public FhirPathMatcher(FhirPathR4 engine,
                           @Qualifier("matcherProperties") Map<String, List<MatcherProperties>> matcherProps) {
        this.matchers = matcherProps
            .values()
            .stream()
            .flatMap(x -> x
                .stream()
                .filter(p -> type.equals(p.getType())))
            .collect(
                Collectors.toMap(MatcherProperties::getTopic, MatcherProperties::getExpression));
        this.engine = engine;
    }

    private List<IBase> match(Bundle bundle, String topic) {

        // lookup match expression for this topic
        var expr = matchers.get(topic);

        return engine.evaluate(bundle, expr, IBase.class);
    }

    @Override
    public Record<String, Bundle> match(Record<String, Bundle> record, String topic) {

        var bundle = record.value();
        var matches = match(bundle, topic);

        if (matches.isEmpty()) {
            return null;
        }

        if (matches.size() == 1 && matches.get(0) instanceof Bundle b) {
            return record.withValue(b);
        }

        var result = new Bundle();
        // Bundle type is 'transaction' by default
        result.setType(BundleType.TRANSACTION);
        result.setMeta(bundle.getMeta());
        result.setEntry(matches
            .stream()
            .map(BundleEntryComponent.class::cast)
            .toList());
        return record.withValue(result);
    }

    @Override
    public boolean supports(String topic) {
        return matchers.containsKey(topic);
    }
}
