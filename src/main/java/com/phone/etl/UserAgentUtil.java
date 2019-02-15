package com.phone.etl;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 功能简述: <br>
 *  解析userAgent信息的工具
 *  window.navigator.userAgent
 * @classname UserAgentUtil
 * @author imyubao
 * @date 2018/09/19
 * @since 1.0
 **/
public class UserAgentUtil {
    private static final Logger logger = Logger.getLogger(UserAgentUtil.class);
    /** 解析userAgent信息的工具类对象 */
    private static UASparser uasParser = null;

    //初始化uaSparser对象
    static{
        try {
            uasParser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.error("获取uasparser异常.",e);
        }
    }

    /**
     * 功能描述: <br>
     *  传入一条userAgent信息字符串，将其各个字段解析出来并封装到自定义的实体类中
     * @param userAgent 要解析的userAgent信息字符串
     * @return com.phone.etl.UserAgentUtil.UserAgentInfo
     * @since 1.0
     * @author imyubao
     * @date 2018/9/19 17:31
     */
    public static UserAgentInfo parserUserAgent(String userAgent){
        UserAgentInfo info = null;
        try {
            if(StringUtils.isNotEmpty(userAgent)){
                //使用uasParser解析
                cz.mallat.uasparser.UserAgentInfo userAgentInfo = uasParser.parse(userAgent);
                if(userAgentInfo != null){
                    info = new UserAgentInfo();
                    //将userAgentInfo中的值取出来赋值给info
                    info.setBrowserName(userAgentInfo.getUaFamily());
                    info.setBrowserVersion(userAgentInfo.getBrowserVersionInfo());
                    info.setOsName(userAgentInfo.getOsFamily());
                    info.setOsVersion(userAgentInfo.getOsName());
                }
            }
        } catch (IOException e) {
            logger.error("解析userAgent异常",e);
        }
        return info;
    }

    /**
     * 功能简述: <br>
     *  用于封装解析后的userAgent各字段信息的实体类
     * @classname UserAgentInfo
     * @author imyubao
     * @date 2018/09/19
     * @since 1.0
     **/
    public static class UserAgentInfo{
        /**
         * userAgent中的各字段，包括浏览器名、浏览器版本、操作系统名、操作系统版本
         */
        private String browserName;
        private String browserVersion;
        private String osName;
        private String osVersion;

        public UserAgentInfo(){
        }

        public UserAgentInfo(String browserName, String browserVersion, String osName, String osVersion) {
            this.browserName = browserName;
            this.browserVersion = browserVersion;
            this.osName = osName;
            this.osVersion = osVersion;
        }

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }
    }
}