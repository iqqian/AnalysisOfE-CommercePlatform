package com.phone.analytic.model.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 功能简述: <br>
 *  浏览器维度实体类
 * @classname BrowserDimension
 * @author imyubao
 * @date 2018/09/20
 * @since 1.0
 **/
public class BrowserDimension extends BaseDimension{
    private int id;
    private String browserName;
    private String browserVersion;

    public BrowserDimension(){
    }

    public BrowserDimension(String browserName, String browserVersion) {
        this.browserName = browserName;
        this.browserVersion = browserVersion;
    }

    public BrowserDimension(int id,String browserName, String browserVersion) {
        this(browserName,browserVersion);
        this.id = id;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.browserName);
        dataOutput.writeUTF(this.browserVersion);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.browserName = dataInput.readUTF();
        this.browserVersion = dataInput.readUTF();
    }


    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
           return 0;
        }

        BrowserDimension other = (BrowserDimension) o;
        int tmp = this.browserName.compareTo(other.browserName);
        if(tmp != 0){
            return tmp;
        }
        return this.browserVersion.compareTo(other.browserVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrowserDimension that = (BrowserDimension) o;
        return id == that.id &&
                Objects.equals(browserName, that.browserName) &&
                Objects.equals(browserVersion, that.browserVersion);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, browserName, browserVersion);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }
}