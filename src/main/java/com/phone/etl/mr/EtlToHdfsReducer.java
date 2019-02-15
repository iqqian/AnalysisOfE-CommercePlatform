/**
 * Copyright (C), 2015-2018
 * FileName: EtlToHdfsReducer
 * Author: imyubao
 * Date: 2018/9/22 22:33
 * Description: ETL数据清洗的reduce阶段，根据event输出到不同的文件中
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.etl.mr;

import com.phone.common.GlobalConstants;
import com.phone.common.LogConstants;
import com.phone.etl.EventOutput;
import com.phone.etl.writable.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 功能简述: <br>
 * ETL数据清洗的reduce阶段，根据event输出到不同的文件中
 *
 * @author imyubao
 * @classname EtlToHdfsReducer
 * @create 2018/9/22
 * @since 1.0
 */
public class EtlToHdfsReducer extends Reducer<Text,LogWritable,NullWritable,EventOutput>{

    private static Logger logger = Logger.getLogger(EtlToHdfsReducer.class);
    private NullWritable k = NullWritable.get();
    /**
     * 设置多文件输出对象
     */
    private MultipleOutputs mos;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //初始化mos
        mos = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<LogWritable> values, Context context) throws IOException, InterruptedException {
        String event = key.toString();
        for (LogWritable logInfo :values){
            String date = context.getConfiguration().get(GlobalConstants.RUNNING_DATE).replaceAll("-","");
            mos.write("logInfo",this.k,logInfo,"logInfo/"+"logInfo"+date);
            switch (LogConstants.EventEnum.valueOfAlias(event)){
                case LAUNCH: mos.write("launchEvent",this.k, new LaunchWritable().buildInstance(logInfo),"launchEvent/"+"launchEvent"+date);break;
                case EVENT: mos.write("eventEvent",this.k, new EventWritable().buildInstance(logInfo),"eventEvent/"+"eventEvent"+date);break;
                case PAGE_VIEW:mos.write("pageViewEvent",this.k, new PageViewWritable().buildInstance(logInfo),"pageViewEvent/"+"pageViewEvent"+date);break;
                case CHARGE_REQUEST:mos.write("chargeRequestEvent",this.k, new ChargeRequestWritable().buildInstance(logInfo),"chargeRequestEvent/"+"chargeRequestEvent"+date);break;
                case CHARGE_SUCCESS: mos.write("chargeSuccessEvent",this.k, new ChargeSuccessWritable().buildInstance(logInfo),"chargeSuccessEvent/"+"chargeSuccessEvent"+date);break;
                case CHARGE_REFUND: mos.write("chargeRefundEvent",this.k, new ChargeRefundWritable().buildInstance(logInfo),"chargeRefundEvent/"+"chargeRefundEvent"+date);break;
                default:
                    break;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //释放资源
        mos.close();
    }
}
