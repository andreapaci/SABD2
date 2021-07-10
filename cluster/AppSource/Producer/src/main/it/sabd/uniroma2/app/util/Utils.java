package it.sabd.uniroma2.app.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {


    public static Date formatStringToDate(String date) {
        try {
            return new SimpleDateFormat("yyyy/MM/dd HH:mm").parse(date);
        } catch (ParseException e) {
            return null;
        }
    }

    public static String formatDateToString(Date date) {

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        String stringDate = dateFormat.format(date);
        return stringDate;
    }

    public static String formatDateToStringMinutes(Date date) {

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm");
        String stringDate = dateFormat.format(date);
        return stringDate;
    }


}
