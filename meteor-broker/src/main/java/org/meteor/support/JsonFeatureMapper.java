package org.meteor.support;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class JsonFeatureMapper {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public static byte[] serialize(Object o) throws JsonProcessingException {
        return MAPPER.writeValueAsBytes(o);
    }

    public static <T> T deserialize(byte[] data, Class<T> clz) throws IOException {
        return MAPPER.readValue(data, clz);
    }

    public static <T> T deserialize(byte[] data, TypeReference<T> clz) throws IOException {
        return MAPPER.readValue(data, clz);
    }
}
