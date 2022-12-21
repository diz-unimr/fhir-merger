package de.unimarburg.diz.fhirmerger.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.hl7.fhir.instance.model.api.IBaseResource;

public class FhirSerde<T extends IBaseResource> implements Serde<T> {

    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;

    public FhirSerde(Class<T> classType) {
        this.serializer = new FhirSerializer<>();
        this.deserializer = new FhirDeserializer<>(classType);
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
