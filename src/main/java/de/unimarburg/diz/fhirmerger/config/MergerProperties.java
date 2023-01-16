package de.unimarburg.diz.fhirmerger.config;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;


@ConfigurationProperties(prefix = "merger")
@Validated
public class MergerProperties {

    private final Output output = new Output();
    private List<MatcherProperties> input;

    public List<MatcherProperties> getInput() {
        return input;
    }

    public void setInput(List<MatcherProperties> input) {
        this.input = input;
    }

    public Output getOutput() {
        return output;
    }

    public static class MatcherProperties {

        private String topic;
        private String expression;
        private String type;


        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getExpression() {
            return expression;
        }

        public void setExpression(String expression) {
            this.expression = expression;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

    }

    public static class Output {

        private String topic;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }
    }
}

