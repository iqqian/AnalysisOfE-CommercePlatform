package com.phone.etl;

import com.phone.common.LogConstants;
import com.phone.etl.ip.IpUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 功能简述: <br>
 *  处理日志数据的工具类
 * @classname LogUtil
 * @author imyubao
 * @date 2018/09/19
 * @since 1.0
 **/
public class LogUtil {
    private static final Logger logger = Logger.getLogger(LogUtil.class);
    /**
     * 功能描述: <br>
     *  解析一行的日志数据，将各个字段及其对应的value添加到map中
     * @param log 要解析的一行的日志数据
     * @return java.util.Map<java.lang.String , java.lang.String>
     * @since 1.0
     * @author imyubao
     * @date 2018/9/19 17:19
     */
    public static Map<String,String> parserLog(String log){
        Map<String,String> map = new ConcurrentHashMap<>();
        if(StringUtils.isNotEmpty(log)){
            String [] fields = log.split(LogConstants.LOG_SEPARATOR);
            if(fields.length == 4){
                //存储
                map.put(LogConstants.LOG_IP,fields[0]);
                map.put(LogConstants.LOG_SERVER_TIME,fields[1].replaceAll("\\.",""));
                //参数列表，单独处理
                String params = fields[3];
                //处理参数
                handleParams(params,map);
                //处理ip解析
                handleIp(map);
                //处理userAgent
                handleUserAgent(map);
            }
        }
        return map;
    }

    /**
     * 功能描述: <br>
     *  从map中获取ip信息，通过ipUtil工具解析出ip的地域信息，添加到map中
     * @param map 存储log数据的map
     * @return void
     * @since 1.0
     * @author imyubao
     * @date 2018/9/19 17:21
     */
    private static void handleIp(Map<String,String> map) {
        if(map.containsKey(LogConstants.LOG_IP)){
            IpUtil.RegionInfo info = IpUtil.getRegionInfoByIp(map.get(LogConstants.LOG_IP));
            //将值存储到map中
            map.put(LogConstants.LOG_COUNTRY,info.getCountry());
            map.put(LogConstants.LOG_PROVINCE,info.getProvince());
            map.put(LogConstants.LOG_CITY,info.getCity());
        }
    }

    /**
     * 功能描述: <br>
     *  处理userAgent
     *  从map中获取userAgent信息，通过userAgentUtil工具解析出对应信息并添加到map中
     * @param map 存储log数据的map
     * @return void
     * @since 1.0
     * @author imyubao
     * @date 2018/9/22 17:22
     */
    private static void handleUserAgent(Map<String,String> map) {
        if(map.containsKey(LogConstants.LOG_USERAGENT)){
            UserAgentUtil.UserAgentInfo info = UserAgentUtil.parserUserAgent(map.get(LogConstants.LOG_USERAGENT));
            //将值存储到map中
            map.put(LogConstants.LOG_BROWSER_NAME,info.getBrowserName());
            map.put(LogConstants.LOG_BROWSER_VERSION,info.getBrowserVersion());
            map.put(LogConstants.LOG_OS_NAME,info.getOsName());
            map.put(LogConstants.LOG_OS_VERSION,info.getOsVersion());
        }
    }

    /**
     * 功能描述: <br>
     *  处理url中所有的请求参数，解析出所有的请求参数，添加到map中
     * @param params 一行记录中含有所有请求参数的字符串
     * @param map 存储log数据的map
     * @return void
     * @since 1.0
     * @author imyubao
     * @date 2018/9/19 17:24
     */
    private static void handleParams(String params, Map<String,String> map) {
        try {
            if(StringUtils.isNotEmpty(params)){
                int index = params.indexOf("?");
                if(index > 0){
                    String[] fields  = params.substring(index+1).split("&");
                    //c_time=1535611600666
                    for (String field:fields) {
                        String[] kvs = field.split("=");
                        //对url进行解码
                        String v = URLDecoder.decode(kvs[1],"utf-8");
                        String k = kvs[0];
                        //判断key是否为空
                        if(StringUtils.isNotEmpty(k)){
                            //存储到map中
                            map.put(k,v);
                        }
                    }
                }
            }
        } catch (UnsupportedEncodingException e) {
            logger.warn("value进行urlDecode解码异常.",e);
        }
    }
}