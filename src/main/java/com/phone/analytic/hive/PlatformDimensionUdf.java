/**
 * Copyright (C), 2015-2018
 * FileName: PlatformDimensionUdf
 * Author: imyubao
 * Date: 2018/9/27 20:21
 * Description: 获取平台维度的id
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.hive;

import com.phone.analytic.model.base.PlatformDimension;
import com.phone.analytic.mr.service.IDimension;
import com.phone.analytic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 功能简述: <br>
 * 获取平台维度的id
 *
 * @author imyubao
 * @classname PlatformDimensionUdf
 * @create 2018/9/27
 * @since 1.0
 */
public class PlatformDimensionUdf extends UDF {

    private IDimension iDimension = new IDimensionImpl();

    public int evaluate(String platform){

        if(StringUtils.isEmpty(platform)){
            platform = GlobalConstants.DEFAULT_VALUE;
        }
        int id = -1;

        try {
            PlatformDimension pl = new PlatformDimension(platform);
            id = iDimension.getDimensionIdByObject(pl);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }
}
