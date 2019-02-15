/**
 * Copyright (C), 2015-2018
 * FileName: ActiveUserMapper
 * Author: imyubao
 * Date: 2018/9/24 11:01
 * Description: 活跃用户指标的map阶段
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.au;

import com.phone.analytic.model.StatsCommonDimension;
import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.base.BrowserDimension;
import com.phone.analytic.model.base.DateDimension;
import com.phone.analytic.model.base.KpiDimension;
import com.phone.analytic.model.base.PlatformDimension;
import com.phone.analytic.model.result.map.TimeMapValue;
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
 * 活跃用户指标的map阶段
 *
 * @author imyubao
 * @classname ActiveUserMapper
 * @since 1.0
 */
public class ActiveUserMapper extends Mapper<LongWritable,Text,StatsUserDimension,TimeMapValue> {

    private static Logger logger = Logger.getLogger(ActiveUserMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeMapValue v = new TimeMapValue();
    private KpiDimension activeUserKpi = new KpiDimension(KpiType.ACTIVE_USER.kpiName);
    private KpiDimension browserActiveUserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_USER.kpiName);
    //private KpiDimension hourlyKpi = new KpiDimension(KpiType.HOURLY_ACTIVE_USER.kpiName);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            logger.warn("Empty record in new user KPI!");
            return;
        }
        //拆分字段
        String[] fields = line.split(LogConstants.DEFAULT_FIELD_SEPARATOR);
        String serverTime = fields[1];
        String uuid = fields[3];
        String platform = fields[13];
        String browserName = fields[24];
        String browserVersion = fields[25];

        //判空
        if (StringUtils.isEmpty(serverTime)||StringUtils.isEmpty(uuid)){
            logger.info("serverTime && uuid is null.serverTime:"+serverTime+". uuid:"+uuid);
            return;
        }
        //构造输出的key
        long sTime = Long.valueOf(serverTime);
        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        DateDimension dateDimension = DateDimension.buildDate(sTime, DateEnum.DAY);
        //设置默认的浏览器对象
        BrowserDimension defaultBrowser = new BrowserDimension(GlobalConstants.DEFAULT_VALUE,GlobalConstants.DEFAULT_VALUE);

        //先封装公共维度对象
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setPlatformDimension(platformDimension);
        statsCommonDimension.setKpiDimension(activeUserKpi);

        this.k.setStatsCommonDimension(statsCommonDimension);
        //设置默认浏览器维度
        this.k.setBrowserDimension(defaultBrowser);
        //设置uuid
        this.v.setId(uuid);
        //设置用户访问时服务器时间
        this.v.setTime(sTime);
        //输出
        context.write(this.k,this.v);

        //this.k.getStatsCommonDimension().setKpiDimension(hourlyKpi);
        context.write(this.k,this.v);

        BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
        statsCommonDimension.setKpiDimension(browserActiveUserKpi);
        this.k.setStatsCommonDimension(statsCommonDimension);
        this.k.setBrowserDimension(browserDimension);
        //输出
        context.write(this.k,this.v);

    }
}
