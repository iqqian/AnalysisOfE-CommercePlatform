package com.phone.utils;

import com.phone.common.DateEnum;
import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 功能简述: <br>
 *  项目中的时间工具类
 * @classname LogWritable
 * @author imyubao
 * @date 2018/09/20
 * @since 1.0
 **/
public class TimeUtil {

    /**
     * 默认的日期格式
     **/
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd";

    /**
     * 功能描述: <br>
     * 以默认日期格式为标准，利用正则表达式判断传入日期字符串格式的合法性
     * @param date 要判断的日期字符串
     * @return boolean 如果可用返回true，否则返回false
     * @since 1.0
     * @author imyubao
     * @date 2018/9/20 17:37
     **/
    public static boolean isValidateDate(String date) {
        return isValidateDate(date,"-");
    }

    /**
     * 功能描述: <br>
     *  指定日期分隔符，利用正则表达式判断传入日期字符串的合法性
     * @param date 要判断的日期字符串
     * @param separator 指定日期分隔符
     * @return boolean 如果可用返回true，否则返回false
     * @since 1.0
     * @author imyubao
     * @date 2018/9/22 20:05
     */
    public static boolean isValidateDate(String date,String separator) {
        Matcher matcher = null;
        boolean res = false;
        String regexp = "^[0-9]{4}"+separator+"[0-9]{1,2}"+separator+"[0-9]{1,2}";
        if (StringUtils.isNotEmpty(date)) {
            Pattern pattern = Pattern.compile(regexp);
            matcher = pattern.matcher(date);
        }
        if (matcher != null) {
            res = matcher.matches();
        }
        return res;
    }

    /**
     * 功能描述: <br>
     *  获取调用时昨天的日期，以默认格式返回一个日期字符串
     * @param
     * @return java.lang.String
     * @since 1.0
     * @author imyubao
     * @date 2018/9/20 17:40
     */
    public static String getNowYesterday() {
        return getNowYesterday(DEFAULT_FORMAT);
    }

    /**
     * 功能描述: <br>
     *  获取调用时昨天的日期，以指定格式返回一个日期字符串
     * @param pattern 指定返回日期的日期格式
     * @return java.lang.String
     * @since 1.0
     * @author imyubao
     * @date 2018/9/20 17:40
     */
    public static String getNowYesterday(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        return getYesterday(sdf.format(calendar.getTime()),pattern);
    }

    /**
     * 功能描述: <br>
     *  传入一个默认格式日期字符串，得到传入时间的昨天，以默认格式返回一个日期字符串
     * @param date
     * @return java.lang.String
     * @since 1.0
     * @author imyubao
     * @date 2018/9/23 20:56
     */
    public static String getYesterday(String date){
        return getYesterday(date,DEFAULT_FORMAT);
    }

    /**
     * 功能描述: <br>
     *  传入一个特定格式的日期字符串，得到传入时间的昨天，以特定格式返回一个日期字符串
     * @param date
     * @param pattern
     * @return java.lang.String
     * @since 1.0
     * @author imyubao
     * @date 2018/9/23 20:57
     */
    public static String getYesterday(String date,String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(parseString2Long(date,pattern));
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        return sdf.format(calendar.getTime());
    }


    /**
     * 功能描述: <br>
     * 将时间戳转换成默认格式的日期字符串
     * @param timestamp 要转换的时间戳
     * @return java.lang.String
     * @since 1.0
     * @author imyubao
     * @date 2018/9/20 17:42
     */
    public static String parseLong2String(long timestamp) {
        return parseLong2String(timestamp, DEFAULT_FORMAT);
    }

    /**
     * 功能描述: <br>
     * 将时间戳转换成指定格式的日期字符串
     * @param timestamp 要转换的时间戳
     * @param pattern 指定返回日期的格式
     * @return java.lang.String
     * @since 1.0
     * @author imyubao
     * @date 2018/9/20 17:44
     */
    public static String parseLong2String(long timestamp, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        return new SimpleDateFormat(pattern).format(calendar.getTime());
    }


    /**
     * 功能描述: <br>
     * 将一个默认格式的日期字符串转换为一个long型的时间戳
     * @param date 默认格式的日期字符串
     * @return long
     * @since 1.0
     * @author imyubao
     * @date 2018/9/20 17:47
     */
    public static long parseString2Long(String date) {
        if (isValidateDate(date)) {
            return parseString2Long(date, DEFAULT_FORMAT);
        }else {
            return 0;
        }
    }

    /**
     * 功能描述: <br>
     * 指定一个日期格式，按照指定格式解析日期字符串转换为一个long型的时间戳
     * @param date 指定格式的日期字符串
     * @param pattern 指定一个日期格式
     * @return long 转换后的long型时间
     * @since 1.0
     * @author imyubao
     * @date 2018/9/20 17:49
     */
    public static long parseString2Long(String date, String pattern) {
        Date dt = null;
        try {
            dt = new SimpleDateFormat(pattern).parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dt.getTime();
    }

    /**
     * 功能描述: <br>
     *  传入一个long型日期，获取指定的日期信息
     * @param time long型的日期
     * @param type 指定要获取的日期信息对应的枚举
     * @return int 返回指定的日期信息
     * @since 1.0
     * @author imyubao
     * @date 2018/9/20 17:52
     */
    public static int getDateInfo(long time, DateEnum type) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if(type.equals(DateEnum.YEAR)){
            return calendar.get(Calendar.YEAR);
        }
        if(type.equals(DateEnum.SEASON)){
            int month = calendar.get(Calendar.MONTH) + 1;
            return month % 3 == 0 ? month / 3 : (month / 3 + 1);
        }
        if(type.equals(DateEnum.MONTH)){
            return calendar.get(Calendar.MONTH) + 1;
        }
        if(type.equals(DateEnum.WEEK)){
            return calendar.get(Calendar.WEEK_OF_YEAR);
        }
        if(type.equals(DateEnum.DAY)){
            return calendar.get(Calendar.DAY_OF_MONTH);
        }
        if(type.equals(DateEnum.HOUR)){
            return calendar.get(Calendar.HOUR_OF_DAY);
        }
        throw  new RuntimeException("不支持该类型的日期信息获取.type："+type.dateType);
    }

    /**
     * 功能描述: <br>
     *  传入一个long型的时间，获取传入时间所在周的第一天的long型时间
     * @param time long型日期
     * @return long
     * @since 1.0
     * @author imyubao
     * @date 2018/9/22 17:54
     */
    public static long getFirstDayOfWeek(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        //该周的第一天
        calendar.set(Calendar.DAY_OF_WEEK,1);
        calendar.set(Calendar.HOUR_OF_DAY,0);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,0);
        calendar.set(Calendar.MILLISECOND,0);
        return calendar.getTimeInMillis();
    }

    /**
     * 功能描述: <br>
     *  传入一个默认格式的日期字符串，对日期中的年月日进行切分，并返回一个数组
     *  如果日期中有个位数，有补0的功能
     *  注意：调用前需先判断日期合法性
     * @param date 要拆分的日期字符串
     * @return java.lang.String[]
     * @since 1.0
     * @author imyubao
     * @date 2018/9/22 19:58
     */
    public static String[] splitDateYMD(String date){
        return splitDateYMD(date,"-");
    }

    /**
     * 功能描述: <br>
     *  按照指定的日期分隔符，将日期中的年月日进行切分，并返回一个数组
     *  如果日期中有个位数，有补0的功能
     *  注意：调用前需先判断日期合法性
     * @param date 要拆分的日期字符串
     * @param separator 指定日期分隔符
     * @return java.lang.String[]
     * @since 1.0
     * @author imyubao
     * @date 2018/9/22 20:07
     */
    public static String[] splitDateYMD(String date,String separator){
        String[] ymd = date.split(separator);
        if (ymd[1].length() == 1){
            ymd[1] = "0"+ymd[1];
        }
        if (ymd[2].length() == 1){
            ymd[2] = "0"+ymd[2];
        }
        return ymd;
    }

    /**
     * 功能描述: <br>
     *  传入一个默认日期格式的字符串，返回一个java.sql.Date对象
     * @param time 传入的字符串
     * @return java.sql.Date
     * @since 1.0
     * @author imyubao
     * @date 2018/9/23 15:48
     */
    public static java.sql.Date getSqlDate(String time){
        Long date = parseString2Long(time);
        return new java.sql.Date(date);
    }



}