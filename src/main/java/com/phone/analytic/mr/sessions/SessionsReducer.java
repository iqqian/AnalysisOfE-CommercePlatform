/**
 * Copyright (C), 2015-2018
 * FileName: SessionsReducer
 * Author: imyubao
 * Date: 2018/9/24 14:48
 * Description: Sessions会话相关指标Reduce阶段
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.sessions;

import com.phone.analytic.model.StatsUserDimension;
import com.phone.analytic.model.result.map.TimeMapValue;
import com.phone.analytic.model.result.reduce.OutputWritable;
import com.phone.analytic.mr.util.TimeChain;
import com.phone.common.DateEnum;
import com.phone.common.GlobalConstants;
import com.phone.common.KpiType;
import com.phone.utils.TimeUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * 功能简述: <br>
 * Sessions会话相关指标Reduce阶段
 *
 * @author imyubao
 * @classname SessionsReducer
 * @create 2018/9/24
 * @since 1.0
 */
public class SessionsReducer extends Reducer<StatsUserDimension,TimeMapValue,
        StatsUserDimension,OutputWritable> {
    private static Logger logger = Logger.getLogger(SessionsReducer.class);
    /**
     * 用来存放session数据，key为sessionId(去重的)，value为该session的最大和最小时间
     */
    private Map<String,TimeChain> timeChainMap = new HashMap();
    private OutputWritable v = new OutputWritable();
    private MapWritable map = new MapWritable();
    /**
     * hourlyTimeChainMap存每小时的所有唯一sessionId和对应的时间链
     * hourlySessions存输出结果每小时的会话个数，hourlySessionLength存每个小时的会话总时长
     */
    private Map<Integer,Map<String,TimeChain>> hourlyTimeChainMap  = new HashMap<>();
    private MapWritable hourlySessions = new MapWritable();
    private MapWritable hourlySessionLength = new MapWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //清空数据
        this.map.clear();
        this.timeChainMap.clear();
        this.hourlySessionLength.clear();
        this.hourlySessions.clear();
        this.hourlyTimeChainMap.clear();
        //初始化
        for (int i=0;i<24;i++){
            this.hourlyTimeChainMap.put(i,new HashMap<>());
            this.hourlySessions.put(new IntWritable(i),new IntWritable(0));
            this.hourlySessionLength.put(new IntWritable(i),new IntWritable(0));
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeMapValue> values, Context context)
            throws IOException, InterruptedException {
        try {
            String kpiName = key.getStatsCommonDimension().getKpiDimension().getKpiName();
            for (TimeMapValue value : values){
                String sessionId = value.getId();
                TimeChain timeChain = timeChainMap.get(sessionId);
                long sTime = value.getTime();

                //判断map中是否存在该session的信息
                if (timeChain == null){
                    timeChain = new TimeChain(sTime);
                    //如果不存在将当前session信息添加到map中
                    timeChainMap.put(sessionId,timeChain);
                }
                //更新时间
                timeChain.addTime(sTime);
                //如果是sessionKpi，在session指标时处理当前小时的会话业务
                if (KpiType.SESSIONS.kpiName.equals(kpiName)){
                    int hour = TimeUtil.getDateInfo(sTime, DateEnum.HOUR);
                    //获取当前小时的所有sessionId和对应的时间链
                    Map<String, TimeChain> nowHourTimeChainMap = hourlyTimeChainMap.get(hour);
                    //获取当前sessionId下的时间链
                    TimeChain nowHourTimeChain = nowHourTimeChainMap.get(sessionId);
                    //如果当前sessionId没有对应的时间链，则构造一个时间链对象，将当前时间添加到其中
                    if (nowHourTimeChain == null){
                        nowHourTimeChain = new TimeChain(sTime);
                        nowHourTimeChainMap.put(sessionId,nowHourTimeChain);
                    }
                    //更新会话时间，保留最大和最小值
                    nowHourTimeChain.addTime(sTime);
                }
            }
            if (KpiType.SESSIONS.kpiName.equals(kpiName)){
                //处理Hourly指标
                handleHourlySessionKpi(key,context);
            }
            //处理BrowserSession和session指标
            handleBrowserAndSessionsKpi(key,context);
        } catch (Exception e) {
            logger.error("SessionKpi计算异常",e);
        }
    }

    /**
     * 功能描述: <br>
     *  处理HourlySessions和HourlySessionLength
     * @param key
     * @param context
     * @return void
     * @since 1.0
     * @author imyubao
     * @date 2018/9/26 17:51
     */
    private void handleHourlySessionKpi(StatsUserDimension key,Context context) throws IOException, InterruptedException {
        long oneHourSessionLength = 0;
        for (Map.Entry<Integer,Map<String,TimeChain>> entry : hourlyTimeChainMap.entrySet()){
            int hour = entry.getKey().intValue();
            for (Map.Entry<String,TimeChain> oneHour : entry.getValue().entrySet()){
                long tmp = oneHour.getValue().getIntervalOfMillis();
                if(tmp < 0 || tmp > GlobalConstants.DAY_OF_MILLISECONDS){
                    //毫秒数小于0或者是大于一天的数将过滤掉
                    continue ;
                }
                oneHourSessionLength += tmp;
                logger.info("for map sessionsLength:"+oneHourSessionLength);
            }
            //计算当前时间总的会话时长，不足一秒记一秒
            oneHourSessionLength = oneHourSessionLength % 1000 == 0 ?
                    oneHourSessionLength / 1000 : oneHourSessionLength / 1000 + 1;

            this.hourlySessions.put(new IntWritable(hour),new IntWritable(entry.getValue().size()));
            this.hourlySessionLength.put(new IntWritable(hour),new IntWritable((int) oneHourSessionLength));
        }
        /**
         * 输出当前小时的会话总个数
         */
        this.v.setKpi(KpiType.HOURLY_SESSIONS);
        this.v.setValue(this.hourlySessions);
        context.write(key,this.v);

        /**
         * 输出当前小时的会话总时长
         */
        this.v.setKpi(KpiType.HOURLY_SESSIONS_LENGTH);
        this.v.setValue(hourlySessionLength);
        //输出结果
        context.write(key,this.v);

    }

    /**
     * 功能描述: <br>
     *  处理browserSessionsKpi和SessionKpi
     * @param key
     * @param context
     * @return void
     * @since 1.0
     * @author imyubao
     * @date 2018/9/26 17:51
     */
    private void handleBrowserAndSessionsKpi(StatsUserDimension key, Context context) throws IOException, InterruptedException {
        //计算总的秒数
        long sessionsLength = 0;
        for (Map.Entry<String, TimeChain> entry : timeChainMap.entrySet()) {
            long tmp = entry.getValue().getIntervalOfMillis();
            if(tmp < 0 || tmp > GlobalConstants.DAY_OF_MILLISECONDS){
                //毫秒数小于0或者是大于一天的数将过滤掉
                continue ;
            }
            sessionsLength += tmp;
            logger.info("for map sessionsLength:"+sessionsLength);
        }
        //计算间隔秒数，不足一秒记一秒
        sessionsLength = sessionsLength % 1000 == 0 ?
                sessionsLength / 1000 : sessionsLength / 1000 + 1;

        //设置kpi类型
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        //封装结果,-1为会话个数，-2为会话总时长
        this.map.put(new IntWritable(-1),new IntWritable(this.timeChainMap.size()));
        this.map.put(new IntWritable(-2),new IntWritable((int) sessionsLength));
        this.v.setValue(this.map);
        //输出结果
        context.write(key,this.v);
    }
}
