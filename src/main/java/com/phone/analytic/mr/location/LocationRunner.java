/**
 * Copyright (C), 2015-2018
 * FileName: LocationRunner
 * Author: imyubao
 * Date: 2018/9/27 9:02
 * Description: 地域指标的驱动类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.location;

import com.phone.analytic.model.StatsLocationDimension;
import com.phone.analytic.model.result.map.LocationMapValue;
import com.phone.analytic.model.result.reduce.LocationOutputWritable;
import com.phone.analytic.mr.MySqlOutputFormat;
import com.phone.analytic.mr.util.HandleMrUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 功能简述: <br>
 * 地域指标的驱动类
 *
 * @author imyubao
 * @classname LocationRunner
 * @create 2018/9/27
 * @since 1.0
 */
public class LocationRunner implements Tool {

    private static Logger logger = Logger.getLogger(LocationRunner.class);
    private Configuration conf = null;

    public static void main(String[] args){
        try {
            ToolRunner.run(new Configuration(),new LocationRunner(),args);
        } catch (Exception e) {
            logger.warn("地域指标执行异常.",e);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"LocationRunner");
        //处理参数，获取-d之后的日期并存储到conf中，如果没有-d或者日期不合法则使用昨天为默认值
        HandleMrUtil.parseRunningDate(job.getConfiguration(),args);
        //设置map相关
        job.setMapperClass(LocationMapper.class);
        job.setMapOutputKeyClass(StatsLocationDimension.class);
        job.setMapOutputValueClass(LocationMapValue.class);

        //设置reduce相关
        job.setReducerClass(LocationReducer.class);

        //设置最终输出key-value类型
        job.setOutputKeyClass(StatsLocationDimension.class);
        job.setOutputValueClass(LocationOutputWritable.class);
        job.setOutputFormatClass(MySqlOutputFormat.class);

        //设置输入路径
        HandleMrUtil.handleInput(job,"logInfo");

        //线程等待
        return job.waitForCompletion(true)?1:0;
    }


    @Override
    public void setConf(Configuration conf) {
        conf.addResource("output_mapping.xml");
        conf.addResource("output_writer.xml");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
