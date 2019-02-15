package com.phone.analytic.model.result;

import com.phone.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * 功能简述: <br>
 *  封装map或者是reduce阶段的输出value的类型的顶级父类
 * @classname BaseStatsValueWritable
 * @author imyubao
 * @date 2018/09/20
 * @since 1.0
 **/
public abstract class BaseStatsValueWritable implements Writable {
    /**
     * 功能描述: <br>
     * 获取kpi枚举类型的抽象方法
     *
     * @param
     * @return com.phone.common.KpiType
     * @since 1.0.0
     * @Author hello
     * @Date 2018/9/20 13:58
     */
    public abstract KpiType getKpi();
}