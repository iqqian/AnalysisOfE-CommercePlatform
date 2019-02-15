package com.phone.analytic.model.base;

import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 功能简述: <br>
 *  平台维度实体类
 * @classname PlatformDimension
 * @author imyubao
 * @date 2018/09/20
 * @since 1.0
 **/
public class PlatformDimension extends BaseDimension{
    private int id;
    private String platformName;

    public PlatformDimension(){}

    public PlatformDimension(String platformName) {
        this.platformName = platformName;
    }

    public PlatformDimension(int id,String patformName) {
        this.platformName = platformName;
        this.id = id;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.platformName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.platformName = dataInput.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }
        PlatformDimension other = (PlatformDimension) o;
        return this.platformName.compareTo(other.platformName);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        PlatformDimension that = (PlatformDimension) o;
        return id == that.id &&
                Objects.equals(platformName, that.platformName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, platformName);
    }

    /**
     * 功能描述: <br>
     *  根据平台名获取平台维度对象
     *
     * @param platformName 平台名
     * @return com.phone.analytic.model.base.PlatformDimension
     * @since 1.0
     * @author imyubao
     * @date 2018/9/22 14:58
     */
    public static PlatformDimension getInstance(String platformName){
        String pl = StringUtils.isEmpty(platformName)? GlobalConstants.DEFAULT_VALUE:platformName;
        return new PlatformDimension(pl);
    }
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }
}