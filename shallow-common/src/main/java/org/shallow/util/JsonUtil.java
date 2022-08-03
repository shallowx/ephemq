package org.shallow.util;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.shallow.meta.Partition;

import javax.naming.OperationNotSupportedException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class JsonUtil {

    private JsonUtil() throws OperationNotSupportedException {
        //unused
        throw new OperationNotSupportedException();
    }

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static String object2Json(Object o) {
        final String content = gson.toJson(o);
        final JsonObject parser = JsonParser.parseString(content).getAsJsonObject();
        return gson.toJson(parser);
    }

    public static Object json2Object(String content, Class<?> clz) {
        return gson.fromJson(content, clz);
    }

    public static <T> T json2Object(String content, Type type) {
        return gson.fromJson(content, type);
    }
}
