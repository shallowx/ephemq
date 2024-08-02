package org.meteor.cli.support;

import java.lang.reflect.Field;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class FormatPrint {
    public static void formatPrint(List<?> objects, String[] title) {
        if (objects == null || objects.isEmpty()) {
            System.out.println(STR."\{currentTime()} [\{Thread.currentThread()
                    .getName()}] ERROR \{FormatPrint.class.getName()} - No format objects were found");
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

    static String currentTime() {
        return DateTimeFormatter.ofPattern("HH:mm:ss.SSS").format(LocalTime.now(ZoneId.of("UTC +08:00")));
    }
}
