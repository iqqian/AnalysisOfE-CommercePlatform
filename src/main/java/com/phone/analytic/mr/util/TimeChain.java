/**
 * Copyright (C), 2015-2018
 * FileName: TimeChain
 * Author: imyubao
 * Date: 2018/9/24 15:15
 * Description: 辅助计算会话时长的工具类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.util;

/**
 * 功能简述: <br>
 * 辅助计算会话时长的工具类
 *
 * @author imyubao
 * @classname TimeChain
 * @since 1.0
 */
public class TimeChain {
    private long minTime;
    private long maxTime;
    private long tempTime;

    public TimeChain(long minTime, long maxTime) {
        this.minTime = minTime;
        this.maxTime = maxTime;
    }

    public TimeChain(long time){
        this.minTime = time;
    }

    public TimeChain(){
    }

    public long getMinTime() {
        return minTime;
    }

    public void setMinTime(long minTime) {
        this.minTime = minTime;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public void setMaxTime(long maxTime) {
        this.maxTime = maxTime;
    }

    public long getTempTime() {
        return tempTime;
    }

    public void setTempTime(long tempTime) {
        this.tempTime = tempTime;
    }

    /**
     * 功能描述: <br>
     * 添加时间，只保存最大时间和最小时间
     * @param time
     * @return void
     * @since 1.0
     * @author imyubao
     * @date 2018/9/24 15:30
     */
    public void addTime(long time){
        if (this.minTime == 0 && this.maxTime == 0){
            this.minTime = time;
        }else if(this.minTime != 0 && this.maxTime == 0){
            if (this.minTime > time){
                this.tempTime = this.minTime;
                this.minTime = time;
                this.maxTime = this.tempTime;
            }else {
                this.maxTime = time;
            }
        }else if (this.minTime != 0 && this.maxTime != 0){
            if (time < this.minTime){
                this.minTime = time;
            }else if (time > this.maxTime){
                this.maxTime = time;
            }
        }
    }

    /**
     * 功能描述: <br>
     *  获取时间间隔，单位 毫秒
     * @param
     * @return long
     * @since 1.0
     * @author imyubao
     * @date 2018/9/24 16:05
     */
    public long getIntervalOfMillis(){
        return this.maxTime - this.minTime;
    }

    /**
     * 功能描述: <br>
     *  获取时间间隔，单位 秒
     * @param
     * @return int
     * @since 1.0
     * @author imyubao
     * @date 2018/9/24 16:05
     */
    public int getIntervalOfSeconds(){
        return (int)(this.getIntervalOfMillis()/1000);
    }
}
