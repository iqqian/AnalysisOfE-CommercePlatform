/**
 * Copyright (C), 2015-2018
 * FileName: EventDimensionUdf
 * Author: imyubao
 * Date: 2018/9/27 20:19
 * Description: 获取事件维度id
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.hive;

import com.phone.analytic.model.base.EventDimension;
import com.phone.analytic.mr.service.IDimension;
import com.phone.analytic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 功能简述: <br>
 * 获取事件维度id
 *
 * @author imyubao
 * @classname EventDimensionUdf
 * @create 2018/9/27
 * @since 1.0
 */
public class EventDimensionUdf extends UDF {

    private IDimension iDimension = new IDimensionImpl();

    public int evaluate(String category,String action){
        if(StringUtils.isEmpty(category)){
            category = action = GlobalConstants.DEFAULT_VALUE;
        }
        if(StringUtils.isEmpty(action)){
            action = GlobalConstants.DEFAULT_VALUE;
        }
        int id = -1;
        try {
            EventDimension ed = new EventDimension(category,action);
            id = iDimension.getDimensionIdByObject(ed);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }

}
