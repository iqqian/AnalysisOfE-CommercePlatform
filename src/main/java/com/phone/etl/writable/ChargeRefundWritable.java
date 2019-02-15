/**
 * Copyright (C), 2015-2018
 * FileName: ChargeRequestWritable
 * Author: imyubao
 * Date: 2018/9/22 22:49
 * Description: 针对chargeRequest事件所需收集数据进行封装的自定义类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.etl.writable;

import com.phone.etl.EventOutput;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 功能简述: <br>
 * 针对chargeRefund事件所需收集数据进行封装的自定义类
 *
 * @author imyubao
 * @classname ChargeRequestWritable
 * @create 2018/9/22
 * @since 1.0
 */
public class ChargeRefundWritable implements EventOutput{

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    @Override
    public EventOutput buildInstance(LogWritable logInfo) {
        return new ChargeRefundWritable();
    }

    @Override
    public EventOutput buildInstance(String info) {
        return null;
    }
}
