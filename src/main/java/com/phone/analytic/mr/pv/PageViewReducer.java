/**
 * Copyright (C), 2015-2018
 * FileName: PageViewReducer
 * Author: imyubao
 * Date: 2018/9/26 20:04
 * Description: PV指标的reduce
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.pv;

import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.result.reduce.OutputWritable;
import com.phone.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 功能简述: <br>
 * PV指标的reduce
 *
 * @author imyubao
 * @classname PageViewReducer
 * @create 2018/9/26
 * @since 1.0
 */
public class PageViewReducer extends Reducer<StatsUserDimension,NullWritable,
        StatsUserDimension,OutputWritable> {
    private OutputWritable v = new OutputWritable();
    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (NullWritable value : values){
            count++;
        }
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        this.map.put(new IntWritable(-1),new IntWritable(count));
        this.v.setValue(map);
        context.write(key,this.v);
    }
}
