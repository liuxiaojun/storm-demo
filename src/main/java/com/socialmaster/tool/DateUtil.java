package com.socialmaster.tool;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by liuxiaojun on 2017/2/21.
 */
public class DateUtil {
    public static long getMinuteDiff(String startMinute, String endMinute) {
        long minutes = -1;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
            long startTimeStamp = sdf.parse(startMinute).getTime();
            long endTimeStamp = sdf.parse(endMinute).getTime();
            minutes = (long)((endTimeStamp-startTimeStamp)/(1000*60));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return minutes;
    }
}