/**
 * Copyright (C), 2015-2018
 * FileName: LocationReducer
 * Author: imyubao
 * Date: 2018/9/27 9:02
 * Description: 地域指标的Reduce类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.mr.location;

import com.phone.analytic.model.StatsLocationDimension;
import com.phone.analytic.model.result.map.LocationMapValue;
import com.phone.analytic.model.result.map.TimeMapValue;
import com.phone.analytic.model.result.reduce.LocationOutputWritable;
import com.phone.analytic.model.result.reduce.OutputWritable;
import com.phone.common.KpiType;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * 功能简述: <br>
 * 地域指标的Reduce类
 *
 * @author imyubao
 * @classname LocationReducer
 * @create 2018/9/27
 * @since 1.0
 */
public class LocationReducer extends Reducer<StatsLocationDimension,LocationMapValue,StatsLocationDimension,OutputWritable>{

    private static final Logger loggwer = Logger.getLogger(LocationReducer.class);
    private LocationOutputWritable v = new LocationOutputWritable();
    /**
     * 存去重uuid
     */
    private  Set unique = new HashSet();
    /**
     * 存sessionId和对应出现次数
     */
    private Map<String,Integer> sessionMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        unique.clear();
        sessionMap.clear();
    }

    @Override
    protected void reduce(StatsLocationDimension key, Iterable<LocationMapValue> values, Context context) throws IOException, InterruptedException {

        for (LocationMapValue value:values){
            this.unique.add(value.getUuid());
            Integer counter = this.sessionMap.get(value.getSessionId());
            if (counter == null){
                this.sessionMap.put(value.getSessionId(),1);
            }else {
                this.sessionMap.put(value.getSessionId(),counter+1);
            }
        }

        int bounceSession = 0;
        for (Map.Entry<String,Integer> entry : sessionMap.entrySet()){
            //如果只出现一次，则为跳出会话
            if (entry.getValue() == 1){
                bounceSession++;
            }
        }

        this.v.setActiveUser(this.unique.size());
        this.v.setBounceSession(bounceSession);
        this.v.setSessions(sessionMap.size());
        this.v.setKpi(KpiType.LOCATION);
        //输出
        context.write(key,this.v);

    }
}
