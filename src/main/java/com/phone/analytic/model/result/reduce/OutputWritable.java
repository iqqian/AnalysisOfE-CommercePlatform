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
 *  封装reduce阶段的输出value基础类
 * @classname OutputWritable
 * @author imyubao
 * @date 2018/09/20
 * @since 1.0
 **/
public class OutputWritable extends BaseStatsValueWritable {
    private KpiType kpi;//kpi是哪个指标。由此可找到对应的数据库表
    private MapWritable value = new MapWritable();

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput,kpi);
        this.value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        WritableUtils.readEnum(dataInput,KpiType.class);
        this.value.readFields(dataInput);
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }
}