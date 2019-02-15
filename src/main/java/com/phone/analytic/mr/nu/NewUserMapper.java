package com.phone.analytic.mr.nu;

import com.phone.analytic.model.StatsCommonDimension;
import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.base.BrowserDimension;
import com.phone.analytic.model.base.DateDimension;
import com.phone.analytic.model.base.KpiDimension;
import com.phone.analytic.model.base.PlatformDimension;
import com.phone.analytic.model.result.map.TimeMapValue;
import com.phone.common.GlobalConstants;
import com.phone.common.LogConstants;
import com.phone.common.DateEnum;
import com.phone.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 功能简述: <br>
 *  用户模块下的新增用户指标map
 * @classname NewUserMapper
 * @author imyubao
 * @since 1.0
 **/
public class NewUserMapper extends Mapper<LongWritable,Text,StatsUserDimension,TimeMapValue> {

    private static  Logger logger = Logger.getLogger(NewUserMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeMapValue v = new TimeMapValue();
    private KpiDimension newUserKpi = new KpiDimension(KpiType.NEW_USER.kpiName);
    private KpiDimension browserNewUserKpi = new KpiDimension(KpiType.BROWSER_NEW_USER.kpiName);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            logger.warn("Empty record in new user KPI!");
            return;
        }
        //拆分字段
        String[] fields = line.split(LogConstants.DEFAULT_FIELD_SEPARATOR);
        //获取事件字段,事件为launch的是新增用户
        String en = fields[2];
        //判断是否是新增用户，如果是，则获取相关字段
        if (StringUtils.isNotEmpty(en) && en.equals(LogConstants.EventEnum.LAUNCH.alias)) {
            //获取相关字段。这个指标需要的字段是：时间、平台、uuid、浏览器名称、浏览器版本
            String serverTime = fields[1];
            String platform = fields[13];
            String uuid = fields[3];
            String browserName = fields[24];
            String browserVersion = fields[25];

            //判空。过滤掉uuid和时间为空的记录
            if (StringUtils.isEmpty(serverTime)||StringUtils.isEmpty(uuid)){
                logger.info("serverTime && uuid is null.serverTime:"+serverTime+". uuid:"+uuid);
                return;
            }
            //构造输出的key。
            // key的组成是：公共维度（时间、平台、kpi）+浏览器维度（用户模块时设置为默认值）
            long sTime = Long.valueOf(serverTime);//时间转化为毫秒
            PlatformDimension platformDimension = PlatformDimension.getInstance(platform);//把具体值封装成平台实例
            DateDimension dateDimension = DateDimension.buildDate(sTime, DateEnum.DAY);//把时间封装成按天的日期维度实例
            //公共维度类
            StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

            //先封装公共维度对象
            statsCommonDimension.setDateDimension(dateDimension);
            statsCommonDimension.setPlatformDimension(platformDimension);
            statsCommonDimension.setKpiDimension(newUserKpi);
            //设置默认的浏览器对象
            BrowserDimension defaultBrowser = new BrowserDimension(GlobalConstants.DEFAULT_VALUE,GlobalConstants.DEFAULT_VALUE);

            this.k.setBrowserDimension(defaultBrowser);
            this.k.setStatsCommonDimension(statsCommonDimension);
            //设置id
            this.v.setId(uuid);

            //输出newUser
            context.write(this.k,this.v);

            ////////////以上是用户模块下的Map的输出，浏览器模块下的指标分析，需要把浏览器的具体值加上//////////////
            //设置数据实际浏览器维度
            BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
            statsCommonDimension.setKpiDimension(browserNewUserKpi);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.k.setBrowserDimension(browserDimension);
            //输出browserNewUser
            context.write(this.k,this.v);
        }

    }
}