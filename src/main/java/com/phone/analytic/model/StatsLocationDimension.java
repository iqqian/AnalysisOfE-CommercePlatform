/**
 * Copyright (C), 2015-2018
 * FileName: StatsLocationDimension
 * Author: imyubao
 * Date: 2018/9/27 9:16
 * Description: 用于地域模块的MR的输出key的类型
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.model;

import com.phone.analytic.model.base.BaseDimension;
import com.phone.analytic.model.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 功能简述: <br>
 * 用于地域模块的MR的输出key的类型
 *
 * @author imyubao
 * @classname StatsLocationDimension
 * @create 2018/9/27
 * @since 1.0
 */
public class StatsLocationDimension extends StatsBaseDimension {

    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private LocationDimension locationDimension = new LocationDimension();

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }
        StatsLocationDimension other = (StatsLocationDimension) o;
        int temp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if (temp != 0){
            return temp;
        }
        return this.locationDimension.compareTo(other.locationDimension);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommonDimension.write(out);
        this.locationDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommonDimension.readFields(in);
        this.locationDimension.readFields(in);
    }

    public StatsLocationDimension() {
    }

    public StatsLocationDimension(StatsCommonDimension statsCommonDimension, LocationDimension locationDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.locationDimension = locationDimension;
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public LocationDimension getLocationDimension() {
        return locationDimension;
    }

    public void setLocationDimension(LocationDimension locationDimension) {
        this.locationDimension = locationDimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsLocationDimension that = (StatsLocationDimension) o;
        return Objects.equals(statsCommonDimension, that.statsCommonDimension) &&
                Objects.equals(locationDimension, that.locationDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(statsCommonDimension, locationDimension);
    }
}
