/**
 * Copyright (C), 2015-2018
 * FileName: LaunchWritable
 * Author: imyubao
 * Date: 2018/9/22 22:46
 * Description: 针对launch事件所需收集的数据进行封装的自定义类
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
 * 针对Launch事件所需收集的数据进行封装的自定义类
 *
 * @author imyubao
 * @classname LaunchWritable
 * @create 2018/9/22
 * @since 1.0
 */
public class LaunchWritable implements EventOutput{

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    @Override
    public EventOutput buildInstance(LogWritable logInfo) {
        return new LaunchWritable();
    }

    @Override
    public EventOutput buildInstance(String info) {
        return null;
    }
}
