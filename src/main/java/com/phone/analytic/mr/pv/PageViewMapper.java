/**
 * Copyright (C), 2015-2018
 * FileName: PageViewMapper
 * Author: imyubao
 * Date: 2018/9/26 20:04
 * Description: PV指标的Mapper
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.pv;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 功能简述: <br>
 * PV指标的Mapper
 *
 * @author imyubao
 * @classname PageViewMapper
 * @create 2018/9/26
 * @since 1.0
 */
public class PageViewMapper extends Mapper<LongWritable,Text,StatsUserDimension,NullWritable> {

    private static Logger logger = Logger.getLogger(PageViewMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private KpiDimension pvKpi = new KpiDimension(KpiType.WEBSITE_PV.kpiName);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            logger.warn("Empty record in PageView KPI!");
            return;
        }
        //拆分字段
        String[] fields = line.split(LogConstants.DEFAULT_FIELD_SEPARATOR);
        String serverTime = fields[1];
        String url = fields[10];
        String platform = fields[13];
        String browserName = fields[24];
        String browserVersion = fields[25];

        //判空
        if (StringUtils.isEmpty(serverTime)||StringUtils.isEmpty(url)){
            logger.info("serverTime && uuid is null.serverTime:"+serverTime+". uuid:"+url);
            return;
        }
        //构造输出的key
        long sTime = Long.valueOf(serverTime);
        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        DateDimension dateDimension = DateDimension.buildDate(sTime, DateEnum.DAY);
        //先封装公共维度对象
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setPlatformDimension(platformDimension);
        statsCommonDimension.setKpiDimension(pvKpi);

        this.k.setStatsCommonDimension(statsCommonDimension);
        BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
        this.k.setBrowserDimension(browserDimension);
        context.write(this.k,NullWritable.get());
    }
}
