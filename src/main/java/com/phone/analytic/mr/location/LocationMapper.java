/**
 * Copyright (C), 2015-2018
 * FileName: LocationMapper
 * Author: imyubao
 * Date: 2018/9/27 9:01
 * Description: 地域指标Mapper类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.location;

import com.phone.analytic.model.StatsCommonDimension;
import com.phone.analytic.model.StatsLocationDimension;
import com.phone.analytic.model.base.DateDimension;
import com.phone.analytic.model.base.KpiDimension;
import com.phone.analytic.model.base.LocationDimension;
import com.phone.analytic.model.base.PlatformDimension;
import com.phone.analytic.model.result.map.LocationMapValue;
import com.phone.common.DateEnum;
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
 * 地域指标Mapper类
 *
 * @author imyubao
 * @classname LocationMapper
 * @create 2018/9/27
 * @since 1.0
 */
public class LocationMapper extends Mapper<LongWritable,Text,StatsLocationDimension,LocationMapValue> {

    private static Logger logger = Logger.getLogger(LocationMapper.class);
    private StatsLocationDimension k = new StatsLocationDimension();
    private LocationMapValue v = new LocationMapValue();
    private KpiDimension locationKpi = new KpiDimension(KpiType.LOCATION.kpiName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            logger.warn("Empty record in Location KPI!");
            return;
        }
        //拆分字段
        String[] fields = line.split(LogConstants.DEFAULT_FIELD_SEPARATOR);
        //获取事件字段
        String en = fields[2];
            //获取相关字段
            String serverTime = fields[1];
            String uuid = fields[3];
            String sessionId = fields[5];
            String platform = fields[13];
            String country = fields[28];
            String province = fields[29];
            String city = fields[30];

            //判空
            if (StringUtils.isEmpty(serverTime)){
                logger.info("serverTime is null.serverTime:"+serverTime);
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
            statsCommonDimension.setKpiDimension(locationKpi);
            //封装地域类
            LocationDimension locationDimension = new LocationDimension(country,province,city);
            //封装k
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.k.setLocationDimension(locationDimension);

            this.v.setSessionId(sessionId);
            this.v.setUuid(uuid);

            context.write(this.k,this.v);

    }

}
