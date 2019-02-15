package com.phone.analytic.model.base;

import org.apache.hadoop.io.WritableComparable;

/**
 * 功能简述: <br>
 *  所有基础维度类的顶级父类
 * @classname BaseDimension
 * @author imyubao
 * @date 2018/09/20
 * @since 1.0
 *
 * SELECT
 * SUM(sdb.new_install_users)
 * FROM stats_device_browser sdb
 * LEFT JOIN dimension_date dd
 * ON dd.id = sdb.date_dimension_id
 * LEFT JOIN dimension_platform dp
 * ON sdb.platform_dimension_id = dp.id
 * LEFT JOIN dimension_browser db
 * ON sdb.browser_dimension_id = db.id
 * WHERE dd.calendar = "2018-09-20"
 * and dp.platform_name = "ios"
 * and db.browser_name = "IE"
 * #GROUP BY sdb.date_dimension_id,sdb.platform_dimension_id,sdb.browser_dimension_id
 * ;
 */
public abstract class BaseDimension implements WritableComparable<BaseDimension> {
    public BaseDimension(){
        super();
    }
}