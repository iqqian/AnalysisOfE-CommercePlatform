/**
 * Copyright (C), 2015-2018
 * FileName: NewUserOutputWriter
 * Author: hello
 * Date: 2018/9/21 14:36
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.nu;

import com.phone.analytic.model.StatsBaseDimension;
import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.result.BaseStatsValueWritable;
import com.phone.analytic.model.result.reduce.OutputWritable;
import com.phone.analytic.mr.IOutputWriter;
import com.phone.analytic.mr.service.IDimension;
import com.phone.common.GlobalConstants;
import com.phone.utils.JdbcUtil;
import com.phone.utils.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 功能简述: <br>
 * 设置NewUser这个kpi的参数。
 * 写入mysql时，对于不同的kpi需要不同的参数设置。在MysqlOutputFormat中调用。
 * @classname NewUserOutputWriter
 * @since 1.0
 **/
public class NewUserOutputWriter implements IOutputWriter {

    @Override
    public void setParams(Configuration conf, StatsBaseDimension key,
                       BaseStatsValueWritable value, IDimension iDimension, PreparedStatement ps) {
        try {
            StatsUserDimension k = (StatsUserDimension) key;//向下转型
            OutputWritable v = (OutputWritable) value;//向下转型
            //根据具体的维度信息获取维度对象对应的id
            String runningDate = conf.get(GlobalConstants.RUNNING_DATE);
            int dateId = iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension());
            int platformId = iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension());
            //从value中获取新增用户数
            int newUser =((IntWritable)v.getValue().get(new IntWritable(-1))).get();
            //获取总用户数
            int totalUser = getTotalUser(newUser,runningDate,dateId,platformId);
            //给参数赋值
            int i = 1;
            ps.setInt(i++,dateId);
            ps.setInt(i++,platformId);
            ps.setInt(i++,newUser);
            ps.setInt(i++,totalUser);
            ps.setDate(i++, TimeUtil.getSqlDate(runningDate));
            ps.setInt(i++,newUser);
            ps.setInt(i++,totalUser);
            ps.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private int getTotalUser(int newUser, String runningDate,int dateId,int platformId) {
        Connection conn = JdbcUtil.getConn();
        PreparedStatement ps = null;
        ResultSet rs = null;
        String sql = "SELECT total_install_users FROM stats_user WHERE platform_dimension_id=? AND created=?";
        int i = 1;

        try {
            ps=conn.prepareStatement(sql);
            ps.setInt(i++,platformId);
            ps.setDate(i++,TimeUtil.getSqlDate(TimeUtil.getYesterday(runningDate)));
            rs = ps.executeQuery();
            if(rs.next()){
                return rs.getInt(1)+newUser;
            }else {
                return newUser;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.close(conn,ps,rs);
        }

        return 0;
    }
}
