/**
 * Copyright (C), 2015-2018
 * FileName: HandleMemberUtil
 * Author: imyubao
 * Date: 2018/9/24 18:31
 * Description: 操作member_info辅助表的工具类，该表主要用于存储访问过的会员id。(该辅助表可以创建为hbase表、redis表等快速查找的key-value库中)  * 以便辅助判断memberid是否是一个正常的umid以及是否是一个新的会员id，是则返回true，反之false。
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.util;

import com.phone.utils.JdbcUtil;
import com.phone.utils.RedisUtil;
import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;


/**
 * 功能简述: <br>
 * 操作member_info辅助表的工具类，该表主要用于存储访问过的会员id。
 * (该辅助表可以创建为HBase表、redis表等快速查找的key-value库中)
 * 以便辅助判断memberId是否是一个正常的u_mid以及是否是一个新的会员id，是则返回true，反之false。
 *
 * @classname HandleMemberUtil
 * @since 1.0
 */
public class HandleMemberUtil {

    private static Jedis jedis;

    static {
        jedis = RedisUtil.getJedis();
    }

    /**
     * 引入缓存，减少数据库查询
     */
    private static Map<String, Boolean> cache = new LinkedHashMap<String, Boolean>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            //当超过10000条记录时移除最早的记录
            return this.size() > 10000;
        }
    };

    /**
     * 功能描述: <br>
     *  判断会员id是否合法
     * @param memberId 会员id
     * @return boolean
     * @since 1.0
     */
    public static boolean isValidateMemberId(String memberId){
        //不为空则判断
        if(StringUtils.isNotBlank(memberId)){
            //进行格式判断
            return memberId.trim().matches("[0-9a-zA-Z]{1,32}");
        }
        return false;
    }

    /**
     * 功能描述: <br>
     *  根据会员id查询数据库判断是否是新用户
     * @param memberId 会员id
     * @param conn 数据库连接对象
     * @return boolean
     * @since 1.0
     */
    public static boolean isNewMemberId(String memberId,Connection conn) throws SQLException{
        Boolean isNewMember = null;
        //判空处理
        if(StringUtils.isNotBlank(memberId)){
            //从缓存中取出value
            isNewMember = cache.get(memberId);
            if(isNewMember == null){
                //表示umid在缓存中没有，去进行数据库查询
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = conn.prepareStatement("select `member_id`,`last_visit_date` from `member_info` where `member_id` = ?");
                    ps.setString(1, memberId);
                    rs = ps.executeQuery();
                    if(rs.next()){
                        //表示数据库中有对应的member_id，则不是新会员，返回false
                        isNewMember = Boolean.valueOf(false);
                    } else {
                        //表示数据库中没有对应的member_id,是新会员
                        isNewMember = Boolean.valueOf(true);
                    }
                    //不管有没有，查一次缓存一次最新的member_id
                    cache.put(memberId, isNewMember);
                } finally {
                    JdbcUtil.close(null,ps,rs);
                }
            }
        }
        //如果value，认定非新会员
        return isNewMember == null ? false : isNewMember.booleanValue();
    }

    ////////////////////////////////Redis/////////////////////

    public static boolean isNewMemberId(String memberId,String date){

        Boolean isNewMember = null;
        jedis.select(1);
        //判空处理
        if(StringUtils.isNotBlank(memberId)){
            //从缓存中取出value
            isNewMember = cache.get(memberId);
            if(isNewMember == null){
                //表示umid在缓存中没有，去进行数据库查询
                if(jedis.keys("*"+memberId).size()>0){
                    isNewMember = false;
                }else {
                    isNewMember = true;
                    jedis.set(date+"_"+memberId,memberId);
                }
                cache.put(memberId,isNewMember);
            }
        }
        if(jedis!=null){
            RedisUtil.returnResource(jedis);
        }
        //如果isNewMember的值为null，就默认是非新会员
        return isNewMember == null ? false : isNewMember.booleanValue();
    }

    /**
     * 功能描述: <br>
     *  删除数据库中指定日期的所有会员信息
     * @param date
     * @return void
     * @since 1.0
     */
    public static void deleteMemberInfoByDate(Connection conn,String date) throws SQLException{
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement("DELETE FROM `member_info` where `created` = ?");
            ps.setString(1,date);
            ps.execute();
        } finally {
            JdbcUtil.close(null,ps,null);
        }
    }

    /**
     * 在redis中删除指定日期的数据
     * @param date
     */
    public static void deleteMemberInfoByDate(String date){
        if(StringUtils.isEmpty(date)){
            return;
        }
        jedis.select(1);
        Set<String> keys = jedis.keys(date+"*");
        for(String k:keys){
            jedis.del(k);
        }
        if(jedis!=null){
            RedisUtil.returnResource(jedis);
        }

    }
}
