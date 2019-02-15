/**
 * Copyright (C), 2015-2018
 * FileName: PageViewOutputWriter
 * Author: imyubao
 * Date: 2018/9/26 20:05
 * Description: Pv指标的输出赋值类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.pv;

import com.phone.analytic.model.StatsBaseDimension;
import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.result.BaseStatsValueWritable;
import com.phone.analytic.model.result.reduce.OutputWritable;
import com.phone.analytic.mr.IOutputWriter;
import com.phone.analytic.mr.service.IDimension;
import com.phone.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 功能简述: <br>
 * Pv指标的输出赋值类
 *
 * @author imyubao
 * @classname PageViewOutputWriter
 * @create 2018/9/26
 * @since 1.0
 */
public class PageViewOutputWriter implements IOutputWriter {

    @Override
    public void setParams(Configuration conf, StatsBaseDimension key, BaseStatsValueWritable value, IDimension iDimension, PreparedStatement ps) {
        try {
            //强制转换获取对应的值
            StatsUserDimension k = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;
            //获取维度对象对应的id
            String runningDate = conf.get(GlobalConstants.RUNNING_DATE);
            int dateId = iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension());
            int platformId = iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension());
            int browserId = iDimension.getDimensionIdByObject(k.getBrowserDimension());
            int pvCounter = ((IntWritable) (v.getValue().get(new IntWritable(-1)))).get();

            //进行参数设置
            int i = 1;
            ps.setInt(i++, dateId);
            ps.setInt(i++, platformId);
            ps.setInt(i++, browserId);
            ps.setInt(i++, pvCounter);
            ps.setString(i++, runningDate);
            ps.setInt(i++, pvCounter);

            //添加到batch中
            ps.addBatch();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
