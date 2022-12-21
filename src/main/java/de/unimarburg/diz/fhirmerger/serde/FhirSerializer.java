package de.unimarburg.diz.fhirmerger.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import de.unimarburg.diz.fhirmerger.config.AppFhirContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Serializer;
import org.hl7.fhir.instance.model.api.IBaseResource;

public class FhirSerializer<T extends IBaseResource> extends JsonSerializer<T> implements
    Serializer<T> {

    @Override
    public byte[] serialize(String topic, IBaseResource data) {
        if (data == null) {
            return null;
        }

        return AppFhirContext
            .getInstance()
            .newJsonParser()
            .encodeResourceToString(data)
            .getBytes(StandardCharsets.UTF_8);
    }


    @Override
    public void serialize(T value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
        var valueString = AppFhirContext
            .getInstance()
            .newJsonParser()
            .encodeResourceToString(value);
        gen.writeString(valueString);
    }
}
