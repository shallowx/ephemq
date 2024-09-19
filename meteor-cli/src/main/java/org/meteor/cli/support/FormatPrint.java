package org.meteor.cli.support;

import java.lang.reflect.Field;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Utility class that provides functionality to format and print details of a list of objects
 * to a tabular text representation.
 */
public class FormatPrint {
    /**
     * Formats and prints the details of the provided list of objects. The fields of the objects
     * to be printed are specified in the title array. If the objects list is null or empty,
     * an error message is printed.
     *
     * @param objects the list of objects to be formatted and printed
     * @param title   array of strings specifying the fields of the objects to be printed
     */
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

    /**
     * Retrieves the current time in the format HH:mm:ss.SSS for the UTC+08:00 timezone.
     *
     * @return A string representing the current time formatted as HH:mm:ss.SSS in the UTC+08:00 timezone.
     */
    static String currentTime() {
        return DateTimeFormatter.ofPattern("HH:mm:ss.SSS").format(LocalTime.now(ZoneId.of("UTC +08:00")));
    }
}
