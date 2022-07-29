package org.shallow.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.naming.OperationNotSupportedException;

public class JsonUtil {

    private JsonUtil() throws OperationNotSupportedException {
        //Unused
        throw new OperationNotSupportedException();
    }

    private static final Gson gson = new GsonBuilder().create();

    public static String object2Json(Object o) {
        return gson.toJson(o);
    }

    public static Object json2Object(String content, Class<?> clz) {
        return gson.fromJson(content, clz);
    }
}
