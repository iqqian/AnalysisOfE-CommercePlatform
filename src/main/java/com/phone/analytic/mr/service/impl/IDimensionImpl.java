package com.phone.analytic.mr.service.impl;

import com.phone.utils.JdbcUtil;
import com.phone.analytic.model.base.*;
import com.phone.analytic.mr.service.IDimension;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 功能简述: <br>
 *  根据维度对象获取对应的id的实现类
 * @classname IDimensionImpl
 * @author imyubao
 * @date 2018/09/21
 * @since 1.0
 **/
public class IDimensionImpl implements IDimension {
    private static Logger logger = Logger.getLogger(IDimensionImpl.class);
    /**
     * 通过LinkedHashMap来实现内存缓存，key为维度信息 value为对应的id
     */
    private Map<String,Integer> cache = new LinkedHashMap<String,Integer>(){
        //当超过五千条记录，自动移除顶端一条记录
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 5000;
        }
    };

    /**
     * 功能描述: <br>
     * 根据传入的维度对象信息，返回相应id的方法实现
     *
     * @param dimension 传入的维度对象
     * @return int
     * @since 1.0
     * @author imyubao
     * @date 2018/9/21 15:31
     */
    @Override
    public int getDimensionIdByObject(BaseDimension dimension)  {
        String cacheKey = buildCacheKey(dimension);
        if (this.cache.containsKey(cacheKey)){
            return this.cache.get(cacheKey);
        }
        Connection conn = null;
        try {
            //得到sql语句数组
            String[] sqls = buildSqls(dimension);
            //获取jdbc连接
            conn = JdbcUtil.getConn();
            int id;
            synchronized (this){
                id = this. executeSql(conn,sqls,dimension);
                //将结果存入cache
                cache.put(cacheKey,id);
                return id;
            }

        } catch (Exception e) {
            logger.error("数据库操作失败！",e);
        } finally {
            JdbcUtil.close(conn,null,null);
        }
        throw new RuntimeException("基础维度数据插入失败！");
    }

    /**
     * 功能描述: <br>
     *  执行sql语句，查询维度对象对应的id，如果未查到
     *  则将当前维度对象的信息插入相应的维度表中并返回对应的id
     *
     * @param conn jdbc连接对象
     * @param sqls sql语句数组，包含查询和插入语句
     * @param dimension 要查询的维度对象
     * @return int 返回维度对象的id
     * @since 1.0
     * @author imyubao
     * @date 2018/9/21 15:34
     */
    private int executeSql(Connection conn, String[] sqls, BaseDimension dimension) {

        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //首先查询，如果查到id返回
            ps = conn.prepareStatement(sqls[0]);
            this.setFields(dimension,ps);
            rs = ps.executeQuery();
            if (rs.next()){
                return rs.getInt(1);
            }

            //如果查询不到，插入数据，返回生成的id
            ps = conn.prepareStatement(sqls[1],Statement.RETURN_GENERATED_KEYS);
            this.setFields(dimension,ps);
            ps.executeUpdate();
            //返回操作后自增的键
            rs = ps.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.error("数据库操作失败",e);
        }finally {
            JdbcUtil.close(null,ps,rs);
        }
        return -1;
    }

    /**
     * 功能描述: <br>
     * 根据维度对象对sql语句赋值
     * @param dimension
     * @param ps
     * @return void
     * @since 1.0
     * @author imyubao
     * @date 2018/9/21 10:46
     */
    private void setFields(BaseDimension dimension, PreparedStatement ps) {
        int i = 1;
        try {
            if (dimension instanceof BrowserDimension){
                BrowserDimension browser = (BrowserDimension) dimension;
                ps.setString(i++,browser.getBrowserName());
                ps.setString(i++,browser.getBrowserVersion());
            } else if (dimension instanceof PlatformDimension){
                PlatformDimension platform = (PlatformDimension) dimension;
                ps.setString(i++,platform.getPlatformName());
            } else if (dimension instanceof KpiDimension){
                KpiDimension kpi = (KpiDimension)dimension;
                ps.setString(i++,kpi.getKpiName());
            } else if (dimension instanceof DateDimension){
                DateDimension date = (DateDimension)dimension;
                ps.setInt(i++, date.getYear());
                ps.setInt(i++, date.getSeason());
                ps.setInt(i++, date.getMonth());
                ps.setInt(i++, date.getWeek());
                ps.setInt(i++, date.getDay());
                ps.setDate(i++, new Date(date.getCalendar().getTime()));
                ps.setString(i++, date.getType());
            } else if (dimension instanceof LocationDimension){
                LocationDimension location = (LocationDimension) dimension;
                ps.setString(i++,location.getCountry());
                ps.setString(i++,location.getProvince());
                ps.setString(i++,location.getCity());
            } else if (dimension instanceof EventDimension){
                EventDimension event = (EventDimension) dimension;
                ps.setString(i++,event.getCategory());
                ps.setString(i++,event.getAction());
            } else{
                throw new RuntimeException("不支持的Dimension"+dimension.getClass());
            }
        } catch (SQLException e) {
            logger.error("ps参数赋值失败",e);
        }
    }

    /**
     * 功能描述: <br>
     * 根据不同的维度对象生成相应的sql语句数组
     *
     * @param dimension 维度对象
     * @return java.lang.String[]
     * @since 1.0.0
     * @author imyubao
     * @date 2018/9/21 10:46
     */
    private String[] buildSqls(BaseDimension dimension) {

        if (dimension instanceof BrowserDimension){
            return buildBrowserSqls();
        } else if (dimension instanceof PlatformDimension){
            return buildPlatformSqls();
        } else if (dimension instanceof KpiDimension){
            return buildKpiSqls();
        } else if (dimension instanceof DateDimension){
            return buildDateSqls();
        } else if (dimension instanceof LocationDimension){
            return buildLocationSqls();
        } else if (dimension instanceof EventDimension){
            return buildEventSqls();
        } else {
            throw new RuntimeException("不支持的Dimension" + dimension.getClass());
        }
    }

    private String[] buildDateSqls() {
        String querySql = "SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `calendar` = ? AND `type` = ?";
        String insertSql = "INSERT INTO `dimension_date`(`year`, `season`, `month`, `week`, `day`,`calendar` , `type`) VALUES(?, ?, ?, ?, ?, ?, ?)";
        return new String[] { querySql, insertSql };
    }

    private String[] buildKpiSqls() {
        String querySql = "SELECT `id` FROM `dimension_kpi` WHERE `kpi_name` = ?";
        String insertSql = "INSERT INTO `dimension_kpi`(`kpi_name`) VALUES(?, ?)";
        return new String[] { querySql, insertSql };
    }

    private String[] buildPlatformSqls() {
        String querySql = "SELECT `id` FROM `dimension_platform` WHERE `platform_name` = ?";
        String insertSql = "INSERT INTO `dimension_platform`(`platform_name`) VALUES(?)";
        return new String[] { querySql, insertSql };
    }

    private String[] buildBrowserSqls() {
        String querySql = "SELECT `id` FROM `dimension_browser` WHERE `browser_name` = ? AND `browser_version` = ?";
        String insertSql = "INSERT INTO `dimension_browser`(`browser_name`, `browser_version`) VALUES(?, ?)";
        return new String[] { querySql, insertSql };
    }

    private String[] buildLocationSqls(){
        String querySql = "SELECT `id` FROM `dimension_location` WHERE `country` = ? AND `province`= ? AND `city`= ?";
        String insertSql = "INSERT INTO `dimension_location`(`country`, `province`,`city`) VALUES(?, ?, ?)";
        return new String[] { querySql, insertSql };
    }

    private String[] buildEventSqls(){
        String querySql = "SELECT `id` FROM `dimension_event` WHERE `category` = ? AND `action`= ?";
        String insertSql = "INSERT INTO `dimension_event`(`category`, `action`) VALUES(?, ?)";
        return new String[] { querySql, insertSql };
    }

    /**
     * 功能描述: <br>
     * 构造维度对象的缓存cacheKey
     * @param dimension 维度对象
     * @return java.lang.String
     * @since 1.0.0
     * @author hello
     * @date 2018/9/21 10:47
     */
    private String buildCacheKey(BaseDimension dimension) {
        //定义一个可变字符串用来拼接cacheKey
        StringBuffer sb = new StringBuffer();

        if (dimension instanceof BrowserDimension){
            BrowserDimension browser = (BrowserDimension) dimension;
            sb.append("browser_").append(browser.getBrowserName()).append(browser.getBrowserVersion());
            return sb.toString();
        } else if (dimension instanceof PlatformDimension){
            PlatformDimension platform = (PlatformDimension) dimension;
            sb.append("platform_").append(platform.getPlatformName());
            return sb.toString();
        } else if (dimension instanceof KpiDimension){
            KpiDimension kpi = (KpiDimension)dimension;
            sb.append("kpi_").append(kpi.getKpiName());
            return sb.toString();
        } else if (dimension instanceof DateDimension){
            DateDimension date = (DateDimension)dimension;
            sb.append("date_").append(date.getYear()).append(date.getSeason())
                    .append(date.getMonth()).append(date.getWeek()).append(date.getDay()).append(date.getType());
            return sb.toString();
        } else if (dimension instanceof LocationDimension){
            LocationDimension location = (LocationDimension)dimension;
            sb.append("location_").append(location.getCountry()).append(location.getProvince()).append(location.getCity());
            return sb.toString();
        } else if (dimension instanceof EventDimension){
            EventDimension event = (EventDimension)dimension;
            System.out.println(event.getClass());
            sb.append("event_").append(event.getCategory()).append(event.getAction());
            return sb.toString();
        }
        return null;
    }

}