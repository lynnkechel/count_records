package com.inforefiner.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTimeUtils {

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    /**
     * 获取当前系统时间
     * @return
     */
    public static String getNowTime(){
        Date date = new Date();
        String format = simpleDateFormat.format(date);
        return format;
    }


    /**
     * 获取当前时间所在整点
     * @return
     */
    public static String getHourTime(){
        Date date = new Date();
        String[] split = date.toString().split(" ")[3].split(":");
        return split[0];
    }


    /**
     * 比较两个时间的大小(返回1，第一个参数大，  返回2，第二个参数大,   返回0，时间相同）
     * @param time1
     * @param time2
     * @return
     */
    public static int compareTime(String time1, String time2){
        int compare = 0;
        if(time1 == null||time2 == null||time1.equals("")||time2.equals("")){
            return compare;
        }
        if(time1.equals(time2)){
            return compare;
        }
        Date d1 = null;
        Date d2 = null;
        try {
            d1=simpleDateFormat.parse(time1);
            d2=simpleDateFormat.parse(time2);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if(d1.before(d2)){
            compare = 2;
        } else {
            compare = 1;
        }
        return compare;
    }


}


