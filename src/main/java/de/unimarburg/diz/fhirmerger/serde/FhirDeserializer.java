package de.unimarburg.diz.fhirmerger.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import de.unimarburg.diz.fhirmerger.config.AppFhirContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.hl7.fhir.instance.model.api.IBaseResource;

public class FhirDeserializer<T extends IBaseResource> extends JsonDeserializer<T> implements
    Deserializer<T> {

    private final Class<T> classType;

    public FhirDeserializer(Class<T> classType) {
        this.classType = classType;
    }


    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        var parser = AppFhirContext
            .getInstance()
            .newJsonParser();
        return parser.parseResource(classType, new ByteArrayInputStream(data));
    }

    @Override
    public T deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
        return deserialize(p.getValueAsString());
    }

    public T deserialize(String value) {
        return AppFhirContext
            .getInstance()
            .newJsonParser()
            .parseResource(classType, value);
    }

    @Override
    public Class<?> handledType() {
        return classType;
    }
}
