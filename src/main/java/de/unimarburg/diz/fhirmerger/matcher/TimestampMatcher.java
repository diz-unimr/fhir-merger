package de.unimarburg.diz.fhirmerger.matcher;

import de.unimarburg.diz.fhirmerger.config.ConditionalOnMatcher;
import de.unimarburg.diz.fhirmerger.config.MergerProperties.MatcherProperties;
import org.apache.kafka.streams.processor.api.Record;
import org.hl7.fhir.r4.model.Bundle;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@ConditionalOnMatcher(TimestampMatcher.TYPE)
public class TimestampMatcher extends BaseMatcher {

    static final String TYPE = "timestamp";
    private final Pattern pattern = Pattern.compile("(<=|<|=|>=|>)\\s*(.+)");
    private final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .parseDefaulting(ChronoField.NANO_OF_DAY, 0)
            .toFormatter()
            .withZone(ZoneOffset.UTC);
    private final Map<String, Function<Long, Boolean>> matchers;

    public TimestampMatcher(@Qualifier("matcherProperties") List<MatcherProperties> matcherProps) {
        matchers = matcherProps
                .stream()
                .filter(x -> TYPE.equals(x.getType()))
                .collect(Collectors.toMap(MatcherProperties::getTopic,
                        x -> buildMatcher(x.getExpression())));
    }

    @Override
    public boolean supports(String topic) {
        return matchers.containsKey(topic);
    }

    @Override
    public Record<String, Bundle> match(Record<String, Bundle> record, String topic) {

        var matcher = matchers.get(topic);

        if (matcher.apply(record.timestamp())) {
            return record;
        }

        return null;
    }

    private Function<Long, Boolean> buildMatcher(String expression) {
        var matcher = pattern.matcher(expression);
        if (matcher.find()) {
            var op = matcher.group(1);
            var dateString = matcher.group(2);
            var epochSecond = formatter
                    .parse(dateString, Instant::from)
                    .getEpochSecond();

            return switch (op) {
                case "<=" -> t -> t <= epochSecond;
                case "<" -> t -> t < epochSecond;
                case "=" -> t -> t == epochSecond;
                case ">=" -> t -> t >= epochSecond;
                case ">" -> t -> t > epochSecond;
                default -> throw new IllegalArgumentException(
                        "Target expression does not match expected pattern: " + expression);
            };
        }
        throw new IllegalArgumentException(
                "Target expression does not match expected pattern: " + expression);
    }
}
