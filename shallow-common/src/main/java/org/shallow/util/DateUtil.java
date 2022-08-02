package org.shallow.util;

import javax.naming.OperationNotSupportedException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

    private DateUtil() throws OperationNotSupportedException {
        //unused
        throw new OperationNotSupportedException();
    }

    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String date2String(Date date) {
        return FORMAT.format(date);
    }

    public static long date2TimeMillis(String time) throws ParseException {
        return FORMAT.parse(time).getTime();
    }
}
