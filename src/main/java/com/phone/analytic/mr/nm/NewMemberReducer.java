/**
 * Copyright (C), 2015-2018
 * FileName: NewMemberReducer
 * Author: imyubao
 * Date: 2018/9/24 17:44
 * Description: 新增会员指标的reduce类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.nm;

import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.result.map.TimeMapValue;
import com.phone.analytic.model.result.reduce.OutputWritable;
import com.phone.analytic.mr.nu.NewUserReducer;
import com.phone.common.KpiType;
import com.phone.utils.RedisUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.*;

/**
 * 功能简述: <br>
 * 新增会员指标的reduce类
 *
 * @author imyubao
 * @classname NewMemberReducer
 * @since 1.0
 */
public class NewMemberReducer  extends Reducer<StatsUserDimension,TimeMapValue,
        StatsUserDimension,OutputWritable> {
    private static Logger logger = Logger.getLogger(NewUserReducer.class);
    private OutputWritable v = new OutputWritable();
    private MapWritable map = new MapWritable();
    /**
     * 用来存新会员id和服务器时间戳，只保留最早的时间。最早的时间就是会员的注册时间
     */
    private Map<String,Long> midSTime = new HashMap<>();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeMapValue> values, Context context) throws IOException, InterruptedException {
        this.map.clear();
        //循环将会员id和服务器时间加到map中
        Long sTime = null;
        for (TimeMapValue value :values){
            //获取新会员id
            String mid = value.getId();
            //如果map中存在此会员id，更新出现时间，保留最早出现的时间
            if (this.midSTime.containsKey(mid)){
                sTime = this.midSTime.get(mid);
                if (sTime > value.getTime()){
                    sTime = value.getTime();
                }
            }else {
                sTime = value.getTime();
            }
            this.midSTime.put(mid,sTime);
        }

        /**
         * 方法一：使用mysql数据库存储会员信息
         */
        //将新会员信息插入到数据库member_info表中,-1 为新会员id,-3 为新会员id出现的最早时间（注册时间）
        this.v.setKpi(KpiType.INSERT_MEMBER_INFO);
        for (Map.Entry<String ,Long> entry:midSTime.entrySet()){
            this.map.put(new IntWritable(-1),new Text(entry.getKey()));
            this.map.put(new IntWritable(-3),new LongWritable(entry.getValue()));
            this.v.setValue(this.map);
            context.write(key,this.v);
        }

        /**
         * 方法二：使用redis数据库存储会员信息
         *  也可以在HandleMemberUtil类中，判断是否是新会员时直接写入到redis 数据库中
         */
        /*////////////////将新会员信息写入到redis数据库中///////////////
        Jedis jedis = RedisUtil.getJedis();
        for (Map.Entry<String,Long> entry:midSTime.entrySet()){
            String redisKey = entry.getValue()+entry.getKey();
            String redisValue = entry.getKey();
            jedis.set(redisKey,redisValue);
        }
        RedisUtil.returnResource(jedis);*/




        //写出统计的新增会员的数据
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        //封装结果 -2 代表 新会员个数
        this.map.put(new IntWritable(-2),new IntWritable(this.midSTime.size()));
        this.v.setValue(this.map);
        //输出
        context.write(key,this.v);
    }
}
