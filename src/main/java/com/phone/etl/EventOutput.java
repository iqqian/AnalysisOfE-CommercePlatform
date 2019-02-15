package com.phone.etl;

import com.phone.etl.writable.LogWritable;
import org.apache.hadoop.io.Writable;
/**
 * 功能简述: <br>
 * 数据清洗阶段map和rudece的输出value的父类
 *
 * @author imyubao
 * @classname EventOutput
 * @create 2018/9/22
 * @since 1.0
 */
public interface EventOutput extends Writable {
    /**
     * 功能描述: <br>
     *  根据不同的事件类型构建相应的实例对象
     *  传入一个LogWritable对象，其中封装了Log日志中的所有数据，根据不同事件类型，
     *  从对象中获取需要的数据并封装的新的事件对象中。
     * @param logInfo
     * @return com.phone.etl.EventOutput
     * @since 1.0
     * @author imyubao
     * @date 2018/9/22 23:19
     */
    EventOutput buildInstance(LogWritable logInfo);

    /**
     * 功能描述: <br>
     * 根据不同的事件类型构建相应的实例对象
     * 传入一个包含事件信息的字符串，拆分出各字段信息，封装到相应的对象中并返回
     *
     * @param info 含有事件所需信息的字符串
     * @return com.phone.etl.EventOutput
     * @since 1.0
     * @author imyubao
     * @date 2018/9/23 19:33
     */
    EventOutput buildInstance(String info);

}
