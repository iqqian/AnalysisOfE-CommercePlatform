/**
 * Copyright (C), 2015-2018
 * FileName: ActiveMemberMapper
 * Author: imyubao
 * Date: 2018/9/24 18:20
 * Description: 活跃会员指标的map类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.am;

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
 * 活跃会员指标的map类
 *
 * @author imyubao
 * @classname ActiveMemberMapper
 * @since 1.0
 */
public class ActiveMemberMapper extends Mapper<LongWritable,Text,StatsUserDimension,TimeMapValue>{

    private static Logger logger = Logger.getLogger(ActiveMemberMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeMapValue v = new TimeMapValue();
    private KpiDimension activeMemberKpi = new KpiDimension(KpiType.ACTIVE_MEMBER.kpiName);
    private KpiDimension browserActiveMemberKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_MEMBER.kpiName);

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
        String memberId = fields[4];
        String platform = fields[13];
        String browserName = fields[24];
        String browserVersion = fields[25];

        //判空
        if (StringUtils.isEmpty(serverTime)||StringUtils.isEmpty(memberId)){
            logger.info("serverTime && mid is null.serverTime:"+serverTime+". mid:"+memberId);
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
        statsCommonDimension.setKpiDimension(activeMemberKpi);

        this.k.setStatsCommonDimension(statsCommonDimension);
        //设置默认浏览器维度
        this.k.setBrowserDimension(defaultBrowser);

        this.v.setId(memberId);
        //输出
        context.write(this.k,this.v);

        BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
        statsCommonDimension.setKpiDimension(browserActiveMemberKpi);
        this.k.setStatsCommonDimension(statsCommonDimension);
        this.k.setBrowserDimension(browserDimension);
        //输出
        context.write(this.k,this.v);
    }
}
