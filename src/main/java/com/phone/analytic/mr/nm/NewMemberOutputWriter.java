/**
 * Copyright (C), 2015-2018
 * FileName: NewMemberOutputWriter
 * Author: imyubao
 * Date: 2018/9/24 18:17
 * Description: 新增会员指标结果输出的实现类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.nm;

import com.phone.analytic.model.StatsBaseDimension;
import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.result.BaseStatsValueWritable;
import com.phone.analytic.model.result.reduce.OutputWritable;
import com.phone.analytic.mr.IOutputWriter;
import com.phone.analytic.mr.service.IDimension;
import com.phone.common.GlobalConstants;
import com.phone.common.KpiType;
import com.phone.utils.JdbcUtil;
import com.phone.utils.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 功能简述: <br>
 * 新增会员指标结果输出的实现类
 *
 * @author imyubao
 * @classname NewMemberOutputWriter
 * @create 2018/9/24
 * @since 1.0
 */
public class NewMemberOutputWriter implements IOutputWriter {

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
            int totalMember,newMembers;
            int i = 1;
            switch (v.getKpi()){
                case NEW_MEMBER:
                    //获取新会员数
                    newMembers = ((IntWritable) (v.getValue().get(new IntWritable(-2)))).get();
                    //获取总会员数
                    totalMember = getTotalMember(runningDate,newMembers,platformId,0);
                    ps.setInt(i++,dateId);
                    ps.setInt(i++,platformId);
                    ps.setInt(i++,newMembers);
                    ps.setInt(i++,totalMember);
                    ps.setDate(i++, TimeUtil.getSqlDate(runningDate));
                    ps.setInt(i++, newMembers);
                    ps.setInt(i++,totalMember);
                    break;
                case BROWSER_NEW_MEMBER:
                    int browserId = iDimension.getDimensionIdByObject(k.getBrowserDimension());
                    newMembers = ((IntWritable) (v.getValue().get(new IntWritable(-2)))).get();
                    totalMember = getTotalMember(runningDate,newMembers,platformId,browserId);
                    ps.setInt(i++,dateId);
                    ps.setInt(i++,platformId);
                    ps.setInt(i++,browserId);
                    ps.setInt(i++,newMembers);
                    ps.setInt(i++,totalMember);
                    ps.setDate(i++, TimeUtil.getSqlDate(runningDate));
                    ps.setInt(i++,newMembers);
                    ps.setInt(i++,totalMember);
                    break;
                case INSERT_MEMBER_INFO:
                    //插入新会员信息
                    ps.setString(i++,((Text)(v.getValue().get(new IntWritable(-1)))).toString());
                    ps.setDate(i++, TimeUtil.getSqlDate(runningDate));
                    ps.setLong(i++,((LongWritable)(v.getValue().get(new IntWritable(-3)))).get());
                    ps.setDate(i++, TimeUtil.getSqlDate(runningDate));
                    ps.setDate(i++, TimeUtil.getSqlDate(runningDate));
                    ps.setLong(i++,((LongWritable)(v.getValue().get(new IntWritable(-3)))).get());
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

    /**
     * 功能描述: <br>
     *  计算总会员数，即昨天的总会员数+今天的新会员数
     * @param date
     * @param newMembers
     * @param platformId
     * @param browserId
     * @return int
     * @since 1.0
     */
    private int getTotalMember(String date, int newMembers ,int platformId, int browserId) {
        Connection conn = JdbcUtil.getConn();
        PreparedStatement ps = null;
        ResultSet rs = null;
        String sql = "";
        int i = 1;
        try {
            //默认如果browserId为0，则kpi为NEW_MEMBER，如果不为0，则kpi为BROWSER_NEW_MEMBER
            if (browserId != 0){
                sql =  "SELECT total_members FROM stats_device_browser WHERE platform_dimension_id=? AND browser_dimension_id=? AND created=?";
                ps=conn.prepareStatement(sql);
                ps.setInt(i++,platformId);
                ps.setInt(i++,browserId);
                ps.setDate(i++,TimeUtil.getSqlDate(TimeUtil.getYesterday(date)));
            }else {
                sql = "SELECT total_members FROM stats_user WHERE platform_dimension_id=? AND created=?";
                ps=conn.prepareStatement(sql);
                ps.setInt(i++,platformId);
                ps.setDate(i++,TimeUtil.getSqlDate(TimeUtil.getYesterday(date)));
            }
            rs = ps.executeQuery();
            if(rs.next()){
                return rs.getInt(1)+newMembers;
            }else {
                return newMembers;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.close(conn,ps,rs);
        }
        return 0;
    }
}
