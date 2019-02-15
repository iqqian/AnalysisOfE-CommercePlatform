/**
 * Copyright (C), 2015-2018
 * FileName: DateDimensionUdf
 * Author: imyubao
 * Date: 2018/9/27 19:47
 * Description: 获取时间维度id
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.hive;

import com.phone.analytic.model.base.DateDimension;
import com.phone.analytic.mr.service.IDimension;
import com.phone.analytic.mr.service.impl.IDimensionImpl;
import com.phone.common.DateEnum;
import com.phone.utils.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

/**
 * 功能简述: <br>
 * 获取时间维度id
 *
 * @author imyubao
 * @classname DateDimensionUdf
 * @create 2018/9/27
 * @since 1.0
 */
public class DateDimensionUdf extends UDF {

    private IDimension iDimension = new IDimensionImpl();

    public int evaluate(String date){
        if(StringUtils.isEmpty(date)){
            date = TimeUtil.getNowYesterday();
        }
        DateDimension dateDimension = DateDimension.buildDate(TimeUtil.parseString2Long(date), DateEnum.DAY);
        int id = 0;
        try {
            id = iDimension.getDimensionIdByObject(dateDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }

}
