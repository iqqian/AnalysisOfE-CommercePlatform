/**
 * Copyright (C), 2015-2018
 * FileName: ActiveMemberReducer
 * Author: imyubao
 * Date: 2018/9/24 18:20
 * Description: 活跃会员指标的reduce类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.am;

import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.result.map.TimeMapValue;
import com.phone.analytic.model.result.reduce.OutputWritable;
import com.phone.analytic.mr.au.ActiveUserReducer;
import com.phone.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 功能简述: <br>
 * 活跃会员指标的reduce类
 *
 * @author imyubao
 * @classname ActiveMemberReducer
 * @since 1.0
 */
public class ActiveMemberReducer  extends Reducer<StatsUserDimension,TimeMapValue,
        StatsUserDimension,OutputWritable> {
    public static final Logger logger = Logger.getLogger(ActiveMemberReducer.class);
    private OutputWritable v = new OutputWritable();
    private MapWritable map = new MapWritable();
    /**
     * 用set存放mid，进一步实现去重
     */
    private Set unique = new HashSet();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeMapValue> values, Context context) throws IOException, InterruptedException {
        //清空map
        this.map.clear();
        this.unique.clear();
        //循环value，将相同key的所有value中的uuid添加到set中
        for (TimeMapValue value : values){
            this.unique.add(value.getId());
        }

        //构造输出的value
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        //设置值，规定-4代表活跃会员数指标
        this.map.put(new IntWritable(-4),new IntWritable(this.unique.size()));
        this.v.setValue(this.map);
        //输出
        context.write(key,this.v);
    }
}
