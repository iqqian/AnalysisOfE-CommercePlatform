package com.phone.etl.mr;

import com.phone.common.LogConstants;
import com.phone.etl.LogUtil;
import com.phone.etl.writable.LogWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * 功能简述: <br>
 *  清洗log日志数据到HDFS的map阶段
 * @classname EtlToHdfsMapper
 * @author imyubao
 * @date 2018/09/19
 * @since 1.0
 **/
public class EtlToHdfsMapper extends Mapper<LongWritable,Text,Text,LogWritable> {
    private static final Logger logger = Logger.getLogger(EtlToHdfsMapper.class);
    private Text k = new Text();
    private LogWritable v = new LogWritable();
    private int inputRecords,filterRecords,outputRecords = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
            this.inputRecords ++;
            //空行处理
            if(StringUtils.isEmpty(line)){
                this.filterRecords ++;
                return ;
            }

            //调用logUtil中的parseLog()方法，返回map，然后循环map将数据分别输出
            //可以根据事件来分别输出？？？
            Map<String,String> map = LogUtil.parserLog(line);

            //获取事件的名
            String eventName = map.get(LogConstants.LOG_EVENT_NAME);
            LogConstants.EventEnum event = LogConstants.EventEnum.valueOfAlias(eventName);

            //处理输出
            handleLog(event,map,context);
        } catch (Exception e) {
            this.filterRecords ++;
            logger.warn("处理mapper写出数据的时候异常.",e);
        }
    }

    /**
     * 功能描述: <br>
     *  处理日志数据，将各字段封装到自定义日志数据实体类中
     * @param map 存放日志数据的集合
     * @param context 上下文对象
     * @return void
     * @since 1.0
     * @author hello
     * @date 2018/9/19 16:47
     */
    private void handleLog(LogConstants.EventEnum event,Map<String,String> map, Context context) {
        try {
            //map循环
            for(Map.Entry<String,String> entry : map.entrySet()){
                switch (entry.getKey()){
                    case "ver": this.v.setVer(entry.getValue()); break;
                    case "s_time": this.v.setS_time(entry.getValue()); break;
                    case "en": this.v.setEn(entry.getValue()); break;
                    case "u_ud": this.v.setU_ud(entry.getValue()); break;
                    case "u_mid": this.v.setU_mid(entry.getValue()); break;
                    case "u_sd": this.v.setU_sd(entry.getValue()); break;
                    case "c_time": this.v.setC_time(entry.getValue()); break;
                    case "l": this.v.setL(entry.getValue()); break;
                    case "b_iev": this.v.setB_iev(entry.getValue()); break;
                    case "b_rst": this.v.setB_rst(entry.getValue()); break;
                    case "p_url": this.v.setP_url(entry.getValue()); break;
                    case "p_ref": this.v.setP_ref(entry.getValue()); break;
                    case "tt": this.v.setTt(entry.getValue()); break;
                    case "pl": this.v.setPl(entry.getValue()); break;
                    case "ip": this.v.setIp(entry.getValue()); break;
                    case "oid": this.v.setOid(entry.getValue()); break;
                    case "on": this.v.setOn(entry.getValue()); break;
                    case "cua": this.v.setCua(entry.getValue()); break;
                    case "cut": this.v.setCut(entry.getValue()); break;
                    case "pt": this.v.setPt(entry.getValue()); break;
                    case "ca": this.v.setCa(entry.getValue()); break;
                    case "ac": this.v.setAc(entry.getValue()); break;
                    case "kv_": this.v.setKv_(entry.getValue()); break;
                    case "du": this.v.setDu(entry.getValue()); break;
                    case "browserName": this.v.setBrowserName(entry.getValue()); break;
                    case "browserVersion": this.v.setBrowserVersion(entry.getValue()); break;
                    case "osName": this.v.setOsName(entry.getValue()); break;
                    case "osVersion": this.v.setOsVersion(entry.getValue()); break;
                    case "country": this.v.setCountry(entry.getValue()); break;
                    case "province": this.v.setProvince(entry.getValue()); break;
                    case "city": this.v.setCity(entry.getValue()); break;
                    default: break;
                }
            }
            switch (event){
                case LAUNCH: this.k.set(LogConstants.EventEnum.LAUNCH.alias);break;
                case EVENT: this.k.set(LogConstants.EventEnum.EVENT.alias);break;
                case PAGE_VIEW: this.k.set(LogConstants.EventEnum.PAGE_VIEW.alias);break;
                case CHARGE_REQUEST:this.k.set(LogConstants.EventEnum.CHARGE_REQUEST.alias);break;
                case CHARGE_SUCCESS: this.k.set(LogConstants.EventEnum.CHARGE_SUCCESS.alias);break;
                case CHARGE_REFUND: this.k.set(LogConstants.EventEnum.CHARGE_REFUND.alias);break;
                default:
                    break;
            }
            this.outputRecords ++;
            context.write(k,v);
        } catch (Exception e) {
            logger.warn("etl最终输出异常",e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("inputRecords:"+this.inputRecords+"  filterRecords:"+filterRecords+"  outputRecords:"+outputRecords);
    }
}