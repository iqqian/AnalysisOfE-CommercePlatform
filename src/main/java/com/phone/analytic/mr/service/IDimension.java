package com.phone.analytic.mr.service;

import com.phone.analytic.model.base.BaseDimension;

import java.io.IOException;
import java.sql.SQLException;

/**
 * 功能简述: <br>
 *  根据维度对象获取对应的id的接口
 * @classname IDimension
 * @author imyubao
 * @date 2018/09/20
 * @since 1.0
 **/
public interface IDimension {

    /**
     * 功能描述: <br>
     *  根据传入的维度对象信息从Mysql中获取相应的ID值
     *  如果没有，则将该维度对象的信息插入到Mysql中，并返回ID值
     *
     * @param dimension 维度对象
     * @return int
     * @throws IOException
     * @throws SQLException
     * @since 1.0
     * @author hello
     * @date 2018/9/22 15:25
     */
    int getDimensionIdByObject(BaseDimension dimension) throws IOException,SQLException;
}