package org.meteor.support;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Utility class for serialization and deserialization of objects using Jackson.
 * <p>
 * This class provides methods to serialize objects to byte arrays and
 * deserialize byte arrays back to objects. It is configured to:
 * - Ignore unknown properties during deserialization.
 * - Only include non-null properties during serialization.
 * - Use a default date format of "yyyy-MM-dd HH:mm:ss.SSS".
 */
public class SerializeFeatureSupport {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
    }

    /**
     * Serializes an object into a byte array using the Jackson ObjectMapper.
     *
     * @param o the object to be serialized
     * @return a byte array representing the serialized object
     * @throws JsonProcessingException if there is a problem during serialization
     */
    public static byte[] serialize(Object o) throws JsonProcessingException {
        return MAPPER.writeValueAsBytes(o);
    }

    /**
     * Deserializes a byte array into an object of the specified class.
     *
     * @param data The byte array to be deserialized.
     * @param clz  The target class of the object to be deserialized.
     * @param <T>  The type of the object to be deserialized.
     * @return The deserialized object of type T.
     * @throws IOException If an error occurs during deserialization.
     */
    public static <T> T deserialize(byte[] data, Class<T> clz) throws IOException {
        return MAPPER.readValue(data, clz);
    }

    /**
     * Deserializes a byte array into an object of the specified type using Jackson.
     *
     * @param <T>  the type of the object to be deserialized
     * @param data the byte array to deserialize
     * @param clz  the TypeReference of the object to be deserialized
     * @return the deserialized object of the specified type
     * @throws IOException if the data cannot be deserialized
     */
    public static <T> T deserialize(byte[] data, TypeReference<T> clz) throws IOException {
        return MAPPER.readValue(data, clz);
    }
}
