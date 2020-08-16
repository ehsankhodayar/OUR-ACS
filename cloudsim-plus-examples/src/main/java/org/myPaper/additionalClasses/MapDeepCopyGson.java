package org.myPaper.additionalClasses;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.Map;

public class MapDeepCopyGson {
    public static <K, V> Map getDeepCopy(Map<K, V> original) {
        Gson gson = new Gson();
        String json = gson.toJson(original);

        return gson.fromJson(json, new TypeToken<Map<K, V>>() {}.getType());
    }
}
