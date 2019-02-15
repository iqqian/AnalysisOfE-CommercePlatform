package com.phone.etl.mr;

import com.phone.analytic.mr.util.HandleMrUtil;
import com.phone.etl.EventOutput;
import com.phone.etl.writable.LogWritable;
import com.phone.utils.TimeUtil;
import com.phone.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 功能简述: <br>
 *  清洗log日志数据到HDFS的驱动类
 *  需求：
 *  原数据：/log/09/18
 *  原数据：/log/09/19
 *  清洗后的存储目录: /ods/09/18
 *  清洗后的存储目录: /ods/09/19
 *  yarn jar ./xxx.jar package.classname -d 2018-09-19
 *
 * @classname EtlToHdfsRunner
 * @author imyubao
 * @date 2018/09/19
 * @since 1.0
 **/
public class EtlToHdfsRunner implements Tool {
    private static final Logger logger = Logger.getLogger(EtlToHdfsRunner.class);
    private Configuration conf = null;

    /**
     * 功能描述: <br>
     *  驱动类的程序入口
     * @param args 外界传入参数
     * @return void
     * @since 1.0
     * @author imyubao
     * @date 2018/9/19 16:50
     */
    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new EtlToHdfsRunner(),args);
        } catch (Exception e) {
            logger.warn("执行etl to hdfs异常.",e);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //1、获取-d之后的日期并存储到conf中，如果没有-d或者日期不合法则使用昨天为默认值
        HandleMrUtil.parseRunningDate(conf,args);
        //获取job
        Job job= Job.getInstance(conf,"etl to hdfs");

        job.setJarByClass(EtlToHdfsRunner.class);

        //设置map相关
        job.setMapperClass(EtlToHdfsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LogWritable.class);

        //设置reduce相关
        job.setReducerClass(EtlToHdfsReducer.class);

        //设置最终输出key-value类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(EventOutput.class);
        //获取runningDate
        //设置多文件输出
        MultipleOutputs.addNamedOutput(job,"logInfo", TextOutputFormat.class,NullWritable.class, EventOutput.class);
        MultipleOutputs.addNamedOutput(job,"launchEvent", TextOutputFormat.class,NullWritable.class, EventOutput.class);
        MultipleOutputs.addNamedOutput(job,"eventEvent", TextOutputFormat.class,NullWritable.class, EventOutput.class);
        MultipleOutputs.addNamedOutput(job,"pageViewEvent", TextOutputFormat.class,NullWritable.class, EventOutput.class);
        MultipleOutputs.addNamedOutput(job,"chargeRequestEvent", TextOutputFormat.class,NullWritable.class, EventOutput.class);
        MultipleOutputs.addNamedOutput(job,"chargeSuccessEvent", TextOutputFormat.class,NullWritable.class, EventOutput.class);
        MultipleOutputs.addNamedOutput(job,"chargeRefundEvent", TextOutputFormat.class,NullWritable.class, EventOutput.class);

        //设置输入输出
        this.handleInputOutput(job);

        return job.waitForCompletion(true)?1:0;
    }

    /**
     * 功能描述: <br>
     *  处理任务的输入和输出
     * @param job 任务对象
     * @return void
     * @since 1.0
     * @author imyubao
     * @date 2018/9/19 16:58
     */
    private void handleInputOutput(Job job) {
        String [] ymd = TimeUtil.splitDateYMD(job.getConfiguration().get(GlobalConstants.RUNNING_DATE));
        String year = ymd[0];
        String month = ymd[1];
        String day = ymd[2];

        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            Path inPath = new Path("/"+year+"/"+month+"/"+day);
            Path outPath = new Path("/ods/"+year+"/"+month+"/"+day);
            if(fs.exists(inPath)){
                FileInputFormat.addInputPath(job,inPath);
            } else {
                throw  new RuntimeException("输入路径不存储在.inPath:"+inPath.toString());
            }
            //如果输出路径已经存在，则删除该输出路径的文件夹，避免报错
            if(fs.exists(outPath)){
                fs.delete(outPath,true);
            }
            //设置输出
            FileOutputFormat.setOutputPath(job,outPath);
        } catch (IOException e) {
            logger.warn("设置输入输出路径异常.",e);
        }
    }
}