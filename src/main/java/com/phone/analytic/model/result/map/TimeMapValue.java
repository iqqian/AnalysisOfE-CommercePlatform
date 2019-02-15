package com.phone.analytic.model.result.map;

import com.phone.analytic.model.result.BaseStatsValueWritable;
import com.phone.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 功能简述: <br>
 *  封装map阶段的输出value基础类
 * @classname TimeMapValue
 * @author imyubao
 * @date 2018/09/20
 * @since 1.0
 **/
public class TimeMapValue extends BaseStatsValueWritable {

    /**
      *  对id的泛指，可以是uuid，可以是u_mid，可以是sessionId
      */
    private String id;
    /**
     * 时间戳
     */
    private long time;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.id);
        dataOutput.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.time = dataInput.readLong();
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}