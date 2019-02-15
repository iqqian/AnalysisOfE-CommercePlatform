/**
 * Copyright (C), 2015-2018
 * FileName: ActiveUserOutputWriter
 * Author: imyubao
 * Date: 2018/9/24 11:21
 * Description: 活跃用户指标输出实现
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.au;

import com.phone.analytic.model.StatsBaseDimension;
import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.result.BaseStatsValueWritable;
import com.phone.analytic.model.result.reduce.OutputWritable;
import com.phone.analytic.mr.IOutputWriter;
import com.phone.analytic.mr.service.IDimension;
import com.phone.common.GlobalConstants;
import com.phone.utils.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 功能简述: <br>
 * 活跃用户指标输出实现
 *
 * @author imyubao
 * @classname ActiveUserOutputWriter
 * @since 1.0
 */
public class ActiveUserOutputWriter implements IOutputWriter {

    @Override
    public void setParams(Configuration conf, StatsBaseDimension key, BaseStatsValueWritable value,
                          IDimension iDimension, PreparedStatement ps) {
        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;
            String runningDate = conf.get(GlobalConstants.RUNNING_DATE);
            //获取维度对象对应的id
            int dateId = iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension());
            int platformId = iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension());
            //从value中获取新增用户数
            int activeUser =((IntWritable)v.getValue().get(new IntWritable(-2))).get();
            int i = 1;
            switch (v.getKpi()){
                case ACTIVE_USER:
                    ps.setInt(i++,dateId);
                    ps.setInt(i++,platformId);
                    ps.setInt(i++,activeUser);
                    ps.setDate(i++, TimeUtil.getSqlDate(runningDate));
                    ps.setInt(i++,activeUser);
                    break;
                case BROWSER_ACTIVE_USER:
                    ps.setInt(i++,dateId);
                    ps.setInt(i++,platformId);
                    ps.setInt(i++,iDimension.getDimensionIdByObject(k.getBrowserDimension()));
                    ps.setInt(i++,activeUser);
                    ps.setDate(i++, TimeUtil.getSqlDate(runningDate));
                    ps.setInt(i++,activeUser);
                    break;
                case HOURLY_ACTIVE_USER:
                    ps.setInt(i++,dateId);
                    ps.setInt(i++,platformId);
                    ps.setInt(i++,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getKpiDimension()));
                    for (;i<28;i++){
                        int count = ((IntWritable)(v.getValue().get(new IntWritable(i-4)))).get();
                        ps.setInt(i,count);
                        ps.setInt(i+25,count);
                    }
                    ps.setDate(i,TimeUtil.getSqlDate(runningDate));
                    break;
                default:
                    break;
            }

            ps.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
