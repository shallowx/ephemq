package org.meteor.cli.support;

import java.lang.reflect.Field;
import java.util.List;

public class FormatPrint {
    public static void formatPrint(List<?> objects, String[] title) {
        if (objects == null || objects.isEmpty()) {
            System.out.println("No format objects were found");
        } else {
            String[][] tables = new String[objects.size()][title.length];
            for (int i = 0; i < objects.size(); i++) {
                Object obj = objects.get(i);
                for (int j = 0; j < title.length; j++) {
                    try {
                        Field field = obj.getClass().getDeclaredField(title[j]);
                        field.setAccessible(true);
                        tables[i][j] = String.valueOf(field.get(obj));
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        tables[i][j] = "N/A";
                    }
                }
            }
            System.out.println(new TextTable(title, tables));
        }
    }
}
