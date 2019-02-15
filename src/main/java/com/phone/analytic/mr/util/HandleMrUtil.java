/**
 * Copyright (C), 2015-2018
 * FileName: HandleMrUtil
 * Author: imyubao
 * Date: 2018/9/23 13:55
 * Description: 处理MR阶段传入参数的工具类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.util;

import com.phone.common.GlobalConstants;
import com.phone.utils.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 功能简述: <br>
 * 处理MR阶段传入参数的工具类
 * @since 1.0
 */
public class HandleMrUtil {
    private static Logger logger = Logger.getLogger(HandleMrUtil.class);
    /**
     * 功能描述: <br>
     *  解析传入参数中的运行日期，并将其设置到conf中
     * @param conf hadoop的配置对象
     * @param args 要处理的传入参数
     * @return void
     * @since 1.0
     */
    public static void parseRunningDate(Configuration conf, String[] args){
        String date = null;
        if(args.length > 0){
            //循环args
            for(int i = 0 ; i<args.length;i++){
                //判断参数中是否有-d
                if("-d".equals(args[i])){
                    if(i+1 <= args.length){
                        date = args[i+1];
                        break;
                    }
                }
            }

            //判空和日期字符串合法性
            if(StringUtils.isEmpty(date)||!TimeUtil.isValidateDate(date)){
                date = TimeUtil.getNowYesterday();
            }
            //将date存储到conf中
            conf.set(GlobalConstants.RUNNING_DATE,date);
        }
    }

    /**
     * 功能描述: <br>
     *  设置MR的输入路径
     * @param job MR任务对象
     * @param input 指定输入路径最内层目录
     * @return void
     * @since 1.0
     */
    public static void handleInput(Job job, String input) {
        String [] ymd = TimeUtil.splitDateYMD(job.getConfiguration().get(GlobalConstants.RUNNING_DATE));
        String year = ymd[0];
        String month = ymd[1];
        String day = ymd[2];

        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            Path inPath = new Path("/ods/"+year+"/"+month+"/"+day+"/"+input);
            if(fs.exists(inPath)){
                FileInputFormat.addInputPath(job,inPath);
            } else {
                throw  new RuntimeException("输入路径不存储在.inPath:"+inPath.toString());
            }
        } catch (IOException e) {
            logger.error("设置输入路径异常",e);
        }
    }

}
