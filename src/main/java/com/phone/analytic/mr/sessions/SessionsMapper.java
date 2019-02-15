/**
 * Copyright (C), 2015-2018
 * FileName: SessionsMapper
 * Author: imyubao
 * Date: 2018/9/24 14:47
 * Description: Sessions会话相关指标Map
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.sessions;

import com.phone.analytic.model.StatsCommonDimension;
import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.base.BrowserDimension;
import com.phone.analytic.model.base.DateDimension;
import com.phone.analytic.model.base.KpiDimension;
import com.phone.analytic.model.base.PlatformDimension;
import com.phone.analytic.model.result.map.TimeMapValue;
import com.phone.analytic.mr.au.ActiveUserMapper;
import com.phone.common.DateEnum;
import com.phone.common.GlobalConstants;
import com.phone.common.KpiType;
import com.phone.common.LogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 功能简述: <br>
 * Sessions会话相关指标Map阶段
 *
 * @author imyubao
 * @classname SessionsMapper
 * @create 2018/9/24
 * @since 1.0
 */
public class SessionsMapper extends Mapper<LongWritable,Text,StatsUserDimension,TimeMapValue>{

    private static  Logger logger = Logger.getLogger(SessionsMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeMapValue v = new TimeMapValue();
    private KpiDimension sessionKpi = new KpiDimension(KpiType.SESSIONS.kpiName);
    private KpiDimension browserSessionKpi = new KpiDimension(KpiType.BROWSER_SESSIONS.kpiName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            logger.warn("Empty record in SESSIONS KPI!");
            return;
        }
        //拆分字段
        String[] fields = line.split(LogConstants.DEFAULT_FIELD_SEPARATOR);
        String serverTime = fields[1];
        String sessionId = fields[5];
        String platform = fields[13];
        String browserName = fields[24];
        String browserVersion = fields[25];

        //判空
        if (StringUtils.isEmpty(serverTime)||StringUtils.isEmpty(sessionId)){
            logger.info("serverTime && sessionId is null.serverTime:"+serverTime+". sessionId:"+sessionId);
            return;
        }

        //构造输出的key
        long sTime = Long.valueOf(serverTime);
        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        DateDimension dateDimension = DateDimension.buildDate(sTime, DateEnum.DAY);
        //公共维度类
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

        //先封装公共维度对象
        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setPlatformDimension(platformDimension);
        statsCommonDimension.setKpiDimension(sessionKpi);

        //设置默认的浏览器对象
        BrowserDimension defaultBrowser = new BrowserDimension(GlobalConstants.DEFAULT_VALUE,GlobalConstants.DEFAULT_VALUE);
        this.k.setBrowserDimension(defaultBrowser);
        this.k.setStatsCommonDimension(statsCommonDimension);
        //设置id
        this.v.setId(sessionId);
        //设置会话时间戳
        this.v.setTime(sTime);
        //输出newUser
        context.write(this.k,this.v);
        //设置数据实际浏览器维度
        BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
        statsCommonDimension.setKpiDimension(browserSessionKpi);
        this.k.setStatsCommonDimension(statsCommonDimension);
        this.k.setBrowserDimension(browserDimension);
        //输出browserNewUser
        context.write(this.k,this.v);
    }
}
