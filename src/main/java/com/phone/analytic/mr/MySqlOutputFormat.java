/**
 * Copyright (C), 2015-2018
 * FileName: MySqlOutputFormat
 * Author: hello
 * Date: 2018/9/21 12:00
 * Description: 输出到mysql的自定义格式类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr;

import com.phone.utils.JdbcUtil;
import com.phone.analytic.model.StatsBaseDimension;
import com.phone.analytic.model.result.reduce.OutputWritable;
import com.phone.analytic.mr.service.IDimension;
import com.phone.analytic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import com.phone.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 功能简述: <br>
 *  输出到mysql的自定义格式类
 * @classname MySqlOutputFormat
 * @author imyubao
 * @since 1.0
 **/

public class MySqlOutputFormat extends OutputFormat<StatsBaseDimension,OutputWritable> {

    private Logger logger = Logger.getLogger(MySqlOutputFormat.class);
//    DBOutputFormat
    /**
     * 功能描述: <br>
     *  返回一个具体定义如何输出数据的对象，RecordWriter被称为数据的输出器
     *
     * @param taskAttemptContext 任务上下文对象
     * @return org.apache.hadoop.mapreduce.RecordWriter<com.phone.analytic.model.StatsBaseDimension , com.phone.analytic.model.result.reduce.OutputWritable>
     * @since 1.0
     * @author imyubao
     */
    @Override
    public RecordWriter<StatsBaseDimension, OutputWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //获取JDBC连接对象
        Connection conn = JdbcUtil.getConn();
        //获取任务配置
        Configuration conf = taskAttemptContext.getConfiguration();
        //查询维度对象id的对象
        IDimension iDimension = new IDimensionImpl();
        return new MySqlOutputRecordWriter(conf,conn,iDimension);

    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        //检测输出是否存在
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FileOutputCommitter(null,taskAttemptContext);
    }


    /**
     * 功能简述: <br>
     *  用来封装记录写出到mysql的内部类
     * @classname MySqlOutputRecordWriter
     * @author imyubao
     * @since 1.0
     **/
    public static class  MySqlOutputRecordWriter extends RecordWriter<StatsBaseDimension,OutputWritable>{

        private Configuration conf = null;
        private Connection conn = null;
        private IDimension iDimension = null;

        /**
         * 用来缓存kpi及其对应的sql的map，存放的key为KpiType对象，value为对应的sql语句
         */
        private Map<KpiType,PreparedStatement> kpiSqlMap = new HashMap<>();
        /**
         * 用来存放kpi及其对应的出现次数
         */
        private Map<KpiType,Integer> kpiBatchNumMap = new HashMap<>();

        public MySqlOutputRecordWriter(Configuration conf, Connection conn, IDimension iDimension) {
            this.conf = conf;
            this.conn = conn;
            this.iDimension = iDimension;
        }

        /**
         * 功能描述: <br>
         *  当Reduce阶段调用context.write()时，地缝调用的是该方法
         * @param key reduce输出的key
         * @param value reduce输出的value
         * @return void
         * @since 1.0
         * @author imyubao
         */
        @Override
        public void write(StatsBaseDimension key, OutputWritable value) throws IOException, InterruptedException {
            //获取kpi
            KpiType kpi = value.getKpi();
            String sql = this.conf.get(kpi.kpiName);
            PreparedStatement ps = null;
            int count = 1;
            try {
                //如果缓存的map中有此kpi，则直接从map中获取sql语句
                if (kpiSqlMap.containsKey(kpi)){
                    ps=kpiSqlMap.get(kpi);
                    if (kpiBatchNumMap.containsKey(kpi)){
                        //如果计数map中存在当前kpi，出现次数+1
                        count = kpiBatchNumMap.get(kpi).intValue() + 1;
                    }
                }else {
                    //如果没有，从output_mapping中获取sql字符串
                    ps = conn.prepareStatement(sql);
                    //将新增加的sql存到map中
                    kpiSqlMap.put(kpi,ps);
                }
                kpiBatchNumMap.put(kpi,count);

                //为ps赋值准备
                //对于不同的kpi有不同的参数设置方法
                String className = conf.get(GlobalConstants.OUTPUT_COLLECTOR_PREFIX+kpi.kpiName);
                Class<?> clazz = Class.forName(className);
                //创建对象 实现子类必须要有一个无参的构造方法
                IOutputWriter writer = (IOutputWriter)clazz.newInstance();
                //调用setParams设置参数
                writer.setParams(conf,key,value,iDimension,ps);
                //对赋值好的ps进行执行
                //如果kpi出现了50次或出现了50个kpi就批量执行
                if(kpiBatchNumMap.get(kpi)% GlobalConstants.BATCH_NUMBER_KPI == 0){
                    ps.executeBatch();
//                    this.conn.commit(); //自定提交
                    //移除已经执行过的kpi
                    kpiBatchNumMap.remove(kpi);
                }else if(kpiBatchNumMap.size()% GlobalConstants.BATCH_NUMBER_KPI == 0){
                    ps.executeBatch();
                    //移除所有已经执行的kpi
                    kpiBatchNumMap.clear();
                }

            } catch (Exception e) {
                throw new IOException("数据输出异常！",e);
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                for (Map.Entry<KpiType,PreparedStatement> entry : this.kpiSqlMap.entrySet()){
                    //将剩余的sql进行执行
                    entry.getValue().executeBatch();
                }
            } catch (SQLException e) {
                throw new IOException("输出数据异常",e);
            }finally {
                //关闭所有资源
                for (Map.Entry<KpiType,PreparedStatement> entry : kpiSqlMap.entrySet()){
                    JdbcUtil.close(conn,entry.getValue(),null);
                }
            }
        }
    }
}
