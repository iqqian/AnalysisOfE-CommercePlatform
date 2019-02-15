/**
 * Copyright (C), 2015-2018
 * FileName: SessionsOutputWriter
 * Author: imyubao
 * Date: 2018/9/24 14:50
 * Description: session指标输出的实现类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.sessions;

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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 功能简述: <br>
 * session指标输出的实现类
 *
 * @author imyubao
 * @classname SessionsOutputWriter
 * @create 2018/9/24
 * @since 1.0
 */
public class SessionsOutputWriter implements IOutputWriter {

    private static Logger logger = Logger.getLogger(SessionsOutputWriter.class);

    @Override
    public void setParams(Configuration conf, StatsBaseDimension key,
                          BaseStatsValueWritable value, IDimension iDimension, PreparedStatement ps) {
        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;
            String runningDate = conf.get(GlobalConstants.RUNNING_DATE);
            //获取维度对象对应的id
            int dateId = iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension());
            int platformId = iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension());
            //从value中获取会话个数和总时长
            int sessions =((IntWritable)v.getValue().get(new IntWritable(-1))).get();
            int sessionsLength =((IntWritable)v.getValue().get(new IntWritable(-2))).get();
            int i = 1;
            //匹配kpi分别赋值
            switch (v.getKpi()){
                case SESSIONS:
                    ps.setInt(i++,dateId);
                    ps.setInt(i++,platformId);
                    ps.setInt(i++,sessions);
                    ps.setInt(i++,sessionsLength);
                    ps.setDate(i++, TimeUtil.getSqlDate(runningDate));
                    ps.setInt(i++,sessions);
                    ps.setInt(i++,sessionsLength);
                    break;
                case BROWSER_SESSIONS:
                    ps.setInt(i++,dateId);
                    ps.setInt(i++,platformId);
                    ps.setInt(i++,iDimension.getDimensionIdByObject(k.getBrowserDimension()));
                    ps.setInt(i++,sessions);
                    ps.setInt(i++,sessionsLength);
                    ps.setDate(i++, TimeUtil.getSqlDate(runningDate));
                    ps.setInt(i++,sessions);
                    ps.setInt(i++,sessionsLength);
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
