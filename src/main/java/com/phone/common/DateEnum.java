package com.phone.common;


/**
 * 功能简述: <br>
 *  常用日期的枚举类
 * @classname DateEnum
 * @author imyubao
 * @date 2018/09/19
 * @since 1.0
 **/
public enum  DateEnum {
    /**
     * 常用日期枚举
     */
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour")
    ;

    public String dateType;

    DateEnum(String dateType) {
        this.dateType = dateType;
    }

    /**
     * 功能描述: <br>
     *  根据日期类型返回相应的枚举
     * @param type
     * @return com.phone.common.DateEnum
     * @since 1.0
     * @author hello
     * @date 2018/9/22 16:14
     */
    public static DateEnum valueOfDateType(String type){
        for (DateEnum date : values()){
            if(date.dateType.equals(type)){
                return date;
            }
        }
        return null;
    }
}