/**
 * Copyright (C), 2015-2018
 * FileName: LocationMapValue
 * Author: imyubao
 * Date: 2018/9/27 17:20
 * Description: 地域指标Map阶段输出的value
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.model.result.map;

import com.phone.analytic.model.result.BaseStatsValueWritable;
import com.phone.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 功能简述: <br>
 * 地域指标Map阶段输出的value
 *
 * @author imyubao
 * @classname LocationMapValue
 * @create 2018/9/27
 * @since 1.0
 */
public class LocationMapValue extends BaseStatsValueWritable {

    private String sessionId;
    private String uuid;

    @Override
    public KpiType getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.sessionId);
        out.writeUTF(this.uuid);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.sessionId = in.readUTF();
        this.uuid = in.readUTF();
    }

    public LocationMapValue(String sessionId, String uuid) {
        this.sessionId = sessionId;
        this.uuid = uuid;
    }


    public LocationMapValue() {
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

}
