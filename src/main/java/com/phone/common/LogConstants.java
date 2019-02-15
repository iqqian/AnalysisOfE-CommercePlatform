package com.phone.common;

/**
 * 功能简述: <br>
 *  整个项目日志的常量类
 * @classname LogConstants
 * @author imyubao
 * @date 2018/09/19
 * @since 1.0
 **/
public class LogConstants {

    /**
     * 功能简述: <br>
     *  所有事件类型的枚举类
     * @classname EventEnum
     * @author imyubao
     * @date 2018/09/19
     * @since 1.0
     **/
    public enum EventEnum{
        /**
         * 所有事件类型的枚举
         */
        LAUNCH(1,"launch event","e_l"),
        PAGE_VIEW(2,"pageView event","e_pv"),
        EVENT(3,"event name","e_e"),
        CHARGE_REQUEST(4,"charge request event","e_crt"),
        CHARGE_SUCCESS(5,"charge success","e_cs"),
        CHARGE_REFUND(6,"charge refund","e_cr")
        ;

        public int id;
        public String name;
        public String alias;

        EventEnum(int id, String name, String alias) {
            this.id = id;
            this.name = name;
            this.alias = alias;
        }

        /**
         * 功能描述: <br>
         *  根据事件别名获取事件对应的枚举类型
         *
         * @param alia 事件别名
         * @return com.phone.common.LogConstants.EventEnum
         * @since 1.0
         * @author hello
         * @date 2018/9/22 16:12
         */
        public static EventEnum valueOfAlias(String alia){
            for (EventEnum event : values()){
                if(event.alias.equals(alia)){
                   return event;
                }
            }
            throw new RuntimeException("该alias没有对应的枚举.alias:"+alia);
        }
    }

    public static final String LOG_SEPARATOR = "\\^A";

    public static final String DEFAULT_FIELD_SEPARATOR = "\u0001";

    public static final String LOG_VERSION = "ver";

    public static final String LOG_SERVER_TIME = "s_time";

    public static final String LOG_EVENT_NAME = "en";

    public static final String LOG_UUID = "u_ud";

    public static final String LOG_MEMBER_ID = "u_mid";

    public static final String LOG_SESSION_ID = "u_sd";

    public static final String LOG_CLIENT_TIME = "c_time";

    public static final String LOG_LANGUAGE = "l";

    public static final String LOG_USERAGENT = "b_iev";

    public static final String LOG_RESOLUTION = "b_rst";

    public static final String LOG_CURRENT_URL = "p_url";

    public static final String LOG_PREFFER_URL = "p_ref";

    public static final String LOG_TITLE = "tt";

    public static final String LOG_PLATFORM = "pl";

    public static final String LOG_IP = "ip";


    /**
     * 和订单相关
     */
    public static final String LOG_ORDER_ID = "oid";

    public static final String LOG_ORDER_NAME = "on";

    public static final String LOG_CURRENCY_AMOUTN = "cua";

    public static final String LOG_CURRENCY_TYPE = "cut";

    public static final String LOG_PAYMENT_TYPE = "pt";


    /**
     * 事件相关
     * 点击：点击事件 category转发
     * 下单：下单事件
     */
    public static final String LOG_EVENT_CATEGORY = "ca";

    public static final String LOG_EVENT_ACTION = "ac";

    public static final String LOG_EVENT_KV = "kv_";

    public static final String LOG_EVENT_DURATION = "du";

    /**
     * 浏览器相关
     */

    public static final String LOG_BROWSER_NAME = "browserName";

    public static final String LOG_BROWSER_VERSION = "browserVersion";

    public static final String LOG_OS_NAME = "osName";

    public static final String LOG_OS_VERSION = "osVersion";
    /**
     * 地域相关
     */

    public static final String LOG_COUNTRY = "country";

    public static final String LOG_PROVINCE = "province";

    public static final String LOG_CITY = "city";

}