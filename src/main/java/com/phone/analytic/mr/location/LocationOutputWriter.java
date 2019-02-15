/**
 * Copyright (C), 2015-2018
 * FileName: LocationOutputWriter
 * Author: imyubao
 * Date: 2018/9/27 9:03
 * Description: 地域指标输出数据的赋值
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.location;

import com.phone.analytic.model.StatsBaseDimension;
import com.phone.analytic.model.StatsLocationDimension;
import com.phone.analytic.model.result.BaseStatsValueWritable;
import com.phone.analytic.model.result.reduce.LocationOutputWritable;
import com.phone.analytic.mr.IOutputWriter;
import com.phone.analytic.mr.service.IDimension;
import com.phone.common.GlobalConstants;
import com.phone.utils.TimeUtil;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

/**
 * 功能简述: <br>
 * 地域指标输出数据的赋值
 *
 * @author imyubao
 * @classname LocationOutputWriter
 * @create 2018/9/27
 * @since 1.0
 */
public class LocationOutputWriter implements IOutputWriter {

    @Override
    public void setParams(Configuration conf, StatsBaseDimension key, BaseStatsValueWritable value, IDimension iDimension, PreparedStatement ps) {
        StatsLocationDimension k = (StatsLocationDimension) key;
        LocationOutputWritable v = (LocationOutputWritable) value;
        String runningDate = conf.get(GlobalConstants.RUNNING_DATE);
        //获取id
        try {
            int dateId = iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension());
            int platformId =  iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension());
            int locationId = iDimension.getDimensionIdByObject(k.getLocationDimension());
            int sessions = v.getSessions();
            int bounceSession = v.getBounceSession();
            int activeUser = v.getActiveUser();
            int i = 1;
            ps.setInt(i++,dateId);
            ps.setInt(i++,platformId);
            ps.setInt(i++,locationId);
            ps.setInt(i++,activeUser);
            ps.setInt(i++,sessions);
            ps.setInt(i++,bounceSession);
            ps.setDate(i++, TimeUtil.getSqlDate(runningDate));
            ps.setInt(i++,activeUser);
            ps.setInt(i++,sessions);
            ps.setInt(i++,bounceSession);
            ps.addBatch();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
