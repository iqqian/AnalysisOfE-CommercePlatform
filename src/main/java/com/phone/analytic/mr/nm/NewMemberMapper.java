/**
 * Copyright (C), 2015-2018
 * FileName: NewMemberMapper
 * Author: imyubao
 * Date: 2018/9/24 17:44
 * Description: 新增会员指标map类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.nm;

import com.phone.analytic.model.StatsCommonDimension;
import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.base.BrowserDimension;
import com.phone.analytic.model.base.DateDimension;
import com.phone.analytic.model.base.KpiDimension;
import com.phone.analytic.model.base.PlatformDimension;
import com.phone.analytic.model.result.map.TimeMapValue;
import com.phone.analytic.mr.au.ActiveUserMapper;
import com.phone.analytic.mr.util.HandleMemberUtil;
import com.phone.common.DateEnum;
import com.phone.common.GlobalConstants;
import com.phone.common.KpiType;
import com.phone.common.LogConstants;
import com.phone.utils.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * 功能简述: <br>
 * 新增会员指标map类
 *
 * @author imyubao
 * @classname NewMemberMapper
 * @since 1.0
 */
public class NewMemberMapper extends Mapper<LongWritable,Text,StatsUserDimension,TimeMapValue> {

    private static Logger logger = Logger.getLogger(NewMemberMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeMapValue v = new TimeMapValue();
    private KpiDimension newMemberKpi = new KpiDimension(KpiType.NEW_MEMBER.kpiName);
    private KpiDimension browserNewMemberKpi = new KpiDimension(KpiType.BROWSER_NEW_MEMBER.kpiName);
    private Connection conn ;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            /**
             * 删除指定日期的数据。。以防reduce运行一半，
             * 一部分数据写到member_info表中，而整个job失败，
             * 当再次运行时member_id就会被过滤，导致数据统计有误。
             * 所以需要运行前将其指定运行日期的数据清空或者查询时加上日期过滤
             */
            conn = JdbcUtil.getConn();
            HandleMemberUtil.deleteMemberInfoByDate(conn,
                    context.getConfiguration().get(GlobalConstants.RUNNING_DATE));
        } catch (SQLException e) {
            logger.error("删除会员信息异常",e);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            logger.warn("Empty record in NEW_MEMBER KPI!");
            return;
        }
        //拆分字段
        String[] fields = line.split(LogConstants.DEFAULT_FIELD_SEPARATOR);
        //获取会员id
        String memberId = fields[4];
        //过滤非新会员,如果不是新会员就过滤掉
        try {
            if (StringUtils.isEmpty(memberId) || !HandleMemberUtil.isValidateMemberId(memberId) || !HandleMemberUtil.isNewMemberId(memberId,conn)){
                logger.info("会员id为空或不合法。也可能该会员非新会员。memberId："+memberId);
                return;
            }
        } catch (SQLException e) {
            logger.error("查询会员id异常",e);
            throw new RuntimeException("查询会员id异常");
        }
        //如果是新会员就进行处理
        String serverTime = fields[1];
        String platform = fields[13];
        String browserName = fields[24];
        String browserVersion = fields[25];

        //判空
        if (StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(platform)){
            logger.info("serverTime & platform  is null.serverTime:"+serverTime+"platform:"+platform);
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
        statsCommonDimension.setKpiDimension(newMemberKpi);
        //设置浏览器维度
        this.k.setStatsCommonDimension(statsCommonDimension);
        this.k.setBrowserDimension(defaultBrowser);
        //设置会员id和服务器时间
        this.v.setTime(sTime);
        this.v.setId(memberId);
        //输出
        context.write(this.k,this.v);

        //浏览器模块下
        BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
        statsCommonDimension.setKpiDimension(browserNewMemberKpi);
        this.k.setStatsCommonDimension(statsCommonDimension);
        this.k.setBrowserDimension(browserDimension);
        //输出
        context.write(this.k,this.v);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //关闭连接资源
        JdbcUtil.close(conn,null,null);
    }
}
