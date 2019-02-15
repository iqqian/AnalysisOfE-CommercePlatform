package com.phone.analytic.mr;

import com.phone.analytic.model.StatsBaseDimension;
import com.phone.analytic.model.result.BaseStatsValueWritable;
import com.phone.analytic.mr.service.IDimension;
import org.apache.hadoop.conf.Configuration;
import java.sql.PreparedStatement;

/**
 * 功能简述: <br>
 *  操作结果表的接口
 * @classname IOutputWriter
 * @author imyubao
 * @date 2018/09/21
 * @since 1.0
 **/
public interface IOutputWriter {

    /**
     * 功能描述: <br>
     *  用来将结果输出到Mysql数据表中的方法
     *
     * @param conf 配置对象
     * @param key reduce阶段输出的key对象
     * @param value reduce阶段输出的value对象
     * @param iDimension 获取维度对象id的接口
     * @param ps 操作sql的PreparedStatement预处理对象
     * @return void
     * @since 1.0
     * @author imyubao
     * @date 2018/9/22 15:46
     */
    void setParams(Configuration conf, StatsBaseDimension key , BaseStatsValueWritable value,
    IDimension iDimension , PreparedStatement ps);

}
