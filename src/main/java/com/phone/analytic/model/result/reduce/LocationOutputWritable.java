/**
 * Copyright (C), 2015-2018
 * FileName: LocationOutputWritable
 * Author: imyubao
 * Date: 2018/9/27 17:49
 * Description: 地域模块Reduce输出的value
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.model.result.reduce;

import com.phone.analytic.model.result.BaseStatsValueWritable;
import com.phone.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 功能简述: <br>
 * 地域模块Reduce输出的value
 *
 * @author imyubao
 * @classname LocationOutputWritable
 * @create 2018/9/27
 * @since 1.0
 */
public class LocationOutputWritable extends OutputWritable {

    private KpiType kpi;
    private int activeUser ;
    private int sessions;
    private int bounceSession;

    public int getActiveUser() {
        return activeUser;
    }

    public void setActiveUser(int activeUser) {
        this.activeUser = activeUser;
    }

    public int getSessions() {
        return sessions;
    }

    public void setSessions(int sessions) {
        this.sessions = sessions;
    }

    public int getBounceSession() {
        return bounceSession;
    }

    public void setBounceSession(int bounceSession) {
        this.bounceSession = bounceSession;
    }

    @Override
    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeEnum(out,kpi);
        out.writeInt(activeUser);
        out.writeInt(sessions);
        out.writeInt(bounceSession);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        WritableUtils.readEnum(in,KpiType.class);
        this.activeUser = in.readInt();
        this.sessions = in.readInt();
        this.bounceSession = in.readInt();
    }
}
