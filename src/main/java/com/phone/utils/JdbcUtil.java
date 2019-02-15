package com.phone.utils;

import com.phone.common.GlobalConstants;

import java.sql.*;

/**
 * 功能简述: <br>
 *  获取JDBC连接的工具类
 * @classname JdbcUtil
 * @author hello
 * @create 2018/09/20
 * @since 1.0
 */
public class JdbcUtil {

    //静态加载驱动
    static {
        try {
            Class.forName(GlobalConstants.DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 功能描述: <br>
     *  获取jdbc连接
     *
     * @param
     * @return java.sql.Connection
     * @since 1.0
     * @author imyubao
     * @date 2018/9/20 14:18
     */
    public static Connection getConn(){
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(GlobalConstants.URL,GlobalConstants.USER
                    ,GlobalConstants.PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 功能描述: <br>
     *  关闭连接资源
     *
     * @param conn 要关闭的连接对象
     * @param ps 要关闭的PreparedStatement对象
     * @param rs 要关闭的ResultSet对象
     * @return void
     * @since 1.0
     * @author imyubao
     * @date 2018/9/20 14:19
     */
    public static void close(Connection conn, PreparedStatement ps, ResultSet rs){
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                //do nothing
            }
        }

        if(ps != null){
            try {
                ps.close();
            } catch (SQLException e) {
                //do nothing
            }
        }

        if(rs != null){
            try {
                rs.close();
            } catch (SQLException e) {
                //do nothing
            }
        }
    }
}