package de.unimarburg.diz.fhirmerger.config;


import ca.uhn.fhir.context.FhirContext;
import de.unimarburg.diz.fhirmerger.config.MergerProperties.MatcherProperties;
import java.util.List;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@ConfigurationPropertiesScan
@EnableKafkaStreams
public class AppConfig {

    @Bean
    public FhirPathR4 fhirPathEngine() {
        return new FhirPathR4(FhirContext.forR4());
    }

    @Bean
    public List<MatcherProperties> matcherProperties(MergerProperties properties) {
        return properties
            .getInput()
            .stream()
            .toList();
    }
}
