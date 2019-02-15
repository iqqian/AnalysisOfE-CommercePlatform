package com.phone.common;

/**
 * 功能简述: <br>
 *  统计指标的枚举类型
 * @classname KpiType
 * @author imyubao
 * @date 2018/09/19
 * @since 1.0
 **/
public enum KpiType {
    /**
     * 各指标kpi枚举
     * NEW_USER 统计新用户kpi
     * BROWSER_NEW_USER 统计浏览器维度的新用户
     * ACTIVE_USER  统计活跃用户kpi
     * BROWSER_ACTIVE_USER  统计浏览维度的活跃用户的kpi
     * ACTIVE_MEMBER    统计活跃会员的kpi
     * BROWSER_ACTIVE_MEMBER    统计浏览器维度的活跃会员kpi
     * NEW_MEMBER   统计新增会员kpi
     * BROWSER_NEW_MEMBER   统计浏览器维度新增会员
     * INSERT_MEMBER_INFO   会员表插入kpi
     * SESSIONS session kpi
     * BROWSER_SESSIONS 浏览器维度的session kpi
     * HOURLY_ACTIVE_USER 按小时分析活跃用户指标
     * HOURLY_SESSIONS 按小时分析会话指标
     */

    NEW_USER("new_user"),
    BROWSER_NEW_USER("browser_new_user"),
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),
    ACTIVE_MEMBER("active_member"),
    BROWSER_ACTIVE_MEMBER("browser_active_member"),
    NEW_MEMBER("new_member"),
    BROWSER_NEW_MEMBER("browser_new_member"),
    INSERT_MEMBER_INFO("insert_member_info"),
    SESSIONS("sessions"),
    BROWSER_SESSIONS("browser_sessions"),
    HOURLY_ACTIVE_USER("hourly_active_user"),
    HOURLY_SESSIONS("hourly_sessions"),
    HOURLY_SESSIONS_LENGTH("hourly_sessions_length"),
    WEBSITE_PV("website_pv"),
    LOCATION("location")
    ;

    public String kpiName;

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    /**
     * 功能描述: <br>
     * 根据kpi的name获取对应的指标
     * @param name 要转换的kpi名
     * @return com.phone.common.KpiType
     * @since 1.0.0
     * @author imyubao
     * @date 2018/9/22 14:06
     */
    public static KpiType valueOfKpiName(String name){
        for (KpiType kpi : values()){
            if(kpi.kpiName.equals(name)){
                return kpi;
            }
        }
        return null;
    }

}