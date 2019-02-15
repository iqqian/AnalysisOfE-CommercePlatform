/**
 * Copyright (C), 2015-2018
 * FileName: NewUserRunner
 * Author: hello
 * Date: 2018/9/21 15:18
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.nu;

import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.result.map.TimeMapValue;
import com.phone.analytic.model.result.reduce.OutputWritable;
import com.phone.analytic.mr.MySqlOutputFormat;
import com.phone.analytic.mr.util.HandleMrUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 功能简述: <br>
 *  新增用户指标驱动类
 * @classname NewUserRunner
 * @author imyubao
 * @since 1.0
 **/
public class NewUserRunner implements Tool {
    private static Logger logger = Logger.getLogger(NewUserRunner.class);
    private Configuration conf = null;

    public static void main(String[] args){
        try {
            ToolRunner.run(new Configuration(),new NewUserRunner(),args);
        } catch (Exception e) {
            logger.warn("执行计算新增用户指标异常.",e);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"NewUser");
        //处理参数，获取-d之后的日期并存储到conf中，如果没有-d或者日期不合法则使用昨天为默认值
        HandleMrUtil.parseRunningDate(job.getConfiguration(),args);
        //设置map相关
        job.setMapperClass(NewUserMapper.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeMapValue.class);

        //设置reduce相关
        job.setReducerClass(NewUserReducer.class);

        //设置最终输出key-value类型
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(OutputWritable.class);
        job.setOutputFormatClass(MySqlOutputFormat.class);

        //设置输入路径
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
