package org.shallow.util;

import com.google.gson.*;

import javax.naming.OperationNotSupportedException;

public class JsonUtil {

    private JsonUtil() throws OperationNotSupportedException {
        //Unused
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
}
