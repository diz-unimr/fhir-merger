package de.unimarburg.diz.fhirmerger.config;

import de.unimarburg.diz.fhirmerger.config.MergerProperties.MatcherProperties;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionMessage;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class OnMatcherTypeCondition extends SpringBootCondition {

    private static final Bindable<List<MatcherProperties>> MATCHER_LIST = Bindable.listOf(
        MatcherProperties.class);

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context,
        AnnotatedTypeMetadata metadata) {

        String propertyName = "merger.input";
        var propertyType = metadata
            .getAnnotations()
            .get(ConditionalOnMatcher.class)
            .getString("value");

        var boundMatcher = Binder
            .get(context.getEnvironment())
            .bind(propertyName, MATCHER_LIST);
        var messageBuilder = ConditionMessage.forCondition("Matcher type " + propertyType);
        if (boundMatcher.isBound()) {
            if (boundMatcher
                .get()
                .stream()
                .anyMatch(matcher -> propertyType.equals(matcher.getType()))) {
                return ConditionOutcome.match(messageBuilder
                    .found("type")
                    .items(propertyType));
            }
            return ConditionOutcome.noMatch(messageBuilder
                .didNotFind("type")
                .items(propertyType));
        }
        return ConditionOutcome.noMatch(messageBuilder
            .didNotFind("property")
            .items(propertyName));
    }

}
