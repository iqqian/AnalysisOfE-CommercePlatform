/**
 * Copyright (C), 2015-2018
 * FileName: ActiveUserReducer
 * Author: imyubao
 * Date: 2018/9/24 11:01
 * Description: 活跃用户指标的reduce阶段
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.au;

import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.result.map.TimeMapValue;
import com.phone.analytic.model.result.reduce.OutputWritable;
import com.phone.common.DateEnum;
import com.phone.common.KpiType;
import com.phone.utils.TimeUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 功能简述: <br>
 * 活跃用户指标的reduce阶段
 *
 * @classname ActiveUserReducer
 * @since 1.0
 */
public class ActiveUserReducer  extends Reducer<StatsUserDimension,TimeMapValue,
        StatsUserDimension,OutputWritable>{

    public static final Logger logger = Logger.getLogger(ActiveUserReducer.class);
    private OutputWritable v = new OutputWritable();
    private MapWritable map = new MapWritable();
    /**
     * 用set存放uuid，进一步实现去重
     */
    private Set unique = new HashSet();

    /**
     * 按小时统计活跃用户数
     * hourlyUnique中存每天各个小时中的去重uuid
     * hourlyMap存每天各个小时的活跃用户数
     */
    private MapWritable hourlyMap = new MapWritable();
    private Map<Integer,Set<String>> hourlyUnique = new HashMap<>();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //这里用来初始化hourlyMap和hourlyUnique
        for (int i=0;i<24;i++){
            hourlyUnique.put(new Integer(i),new HashSet<>());
            hourlyMap.put(new IntWritable(i),new IntWritable(0));
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeMapValue> values, Context context) throws IOException, InterruptedException {
        try {
            if(KpiType.HOURLY_ACTIVE_USER.kpiName.equals(
                    key.getStatsCommonDimension().getKpiDimension().getKpiName())){
                for (TimeMapValue value : values){
                    //获取小时
                    int hour = TimeUtil.getDateInfo(value.getTime(), DateEnum.HOUR);
                    //将uuid加入set中
                    hourlyUnique.get(hour).add(value.getId());
                }
                this.v.setKpi(KpiType.HOURLY_ACTIVE_USER);
                //设置输出结果
                for (Map.Entry<Integer,Set<String>> entry : hourlyUnique.entrySet()){
                    //将每个时段对应的uuid数取出放入hourlyMap中
                    this.hourlyMap.put(new IntWritable(entry.getKey()),new IntWritable(entry.getValue().size()));
                }
                this.v.setValue(hourlyMap);
                context.write(key,this.v);

            }else {
                //循环value，将相同key的所有value中的uuid添加到set中
                for (TimeMapValue mv : values){
                    this.unique.add(mv.getId());
                }
                //构造输出的value
                this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
                //设置值，规定-2代表活跃用户数指标
                this.map.put(new IntWritable(-2),new IntWritable(this.unique.size()));
                this.v.setValue(this.map);
                //输出
                context.write(key,this.v);
            }
        } catch (Exception e) {
            logger.error("活跃用户数计算异常！",e);
        } finally {
            //清空数据
            this.map.clear();
            this.unique.clear();
            this.hourlyMap.clear();
            this.hourlyUnique.clear();
        }
    }
}
