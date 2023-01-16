package de.unimarburg.diz.fhirmerger.matcher;

public abstract class BaseMatcher implements MatchProcessor {

    public abstract boolean supports(String topic);
}
