package com.phone.analytic.mr.nu;

import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.result.map.TimeMapValue;
import com.phone.analytic.model.result.reduce.OutputWritable;
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
 *  用户模块下的新增用户的reducer阶段
 * @classname NewUserReducer
 * @author imyubao
 * @since 1.0
 **/

public class NewUserReducer extends Reducer<StatsUserDimension,TimeMapValue,
        StatsUserDimension,OutputWritable> {
    private static Logger logger = Logger.getLogger(NewUserReducer.class);
    private OutputWritable v = new OutputWritable();
    private MapWritable map = new MapWritable();
    /**
     * 用set存放uuid，进一步实现去重
     */
    private Set unique = new HashSet();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeMapValue> values, Context context) throws IOException, InterruptedException {
        //清空map
        this.map.clear();
        //循环value，将相同key的所有value中的uuid添加到set中
        for (TimeMapValue mv : values){
            this.unique.add(mv.getId());
        }

        //构造输出的value
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        //设置值，规定-1代表新增用户数指标
        this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        this.v.setValue(this.map);

        //输出
        context.write(key,this.v);
        //清空set
        this.unique.clear();
    }
}