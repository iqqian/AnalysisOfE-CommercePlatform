package com.phone.etl.writable;

import com.phone.common.GlobalConstants;
import com.phone.common.LogConstants;
import com.phone.etl.EventOutput;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 功能简述: <br>
 *  自定义的日志数据类型
 *  对日志中的所有字段数据进行封装
 * @classname LogWritable
 * @author imyubao
 * @date 2018/09/19
 * @since 1.0
 **/
public class LogWritable implements EventOutput {
    private String ver = GlobalConstants.DEFAULT_VALUE;
    private String s_time = GlobalConstants.DEFAULT_VALUE;
    private String en = GlobalConstants.DEFAULT_VALUE;
    private String u_ud = GlobalConstants.DEFAULT_VALUE;
    private String u_mid = GlobalConstants.DEFAULT_VALUE;
    private String u_sd = GlobalConstants.DEFAULT_VALUE;
    private String c_time = GlobalConstants.DEFAULT_VALUE;
    private String l = GlobalConstants.DEFAULT_VALUE;
    private String b_iev = GlobalConstants.DEFAULT_VALUE;
    private String b_rst = GlobalConstants.DEFAULT_VALUE;
    private String p_url = GlobalConstants.DEFAULT_VALUE;
    private String p_ref = GlobalConstants.DEFAULT_VALUE;
    private String tt = GlobalConstants.DEFAULT_VALUE;
    private String pl = GlobalConstants.DEFAULT_VALUE;
    private String ip = GlobalConstants.DEFAULT_VALUE;
    private String oid = GlobalConstants.DEFAULT_VALUE;
    private String on = GlobalConstants.DEFAULT_VALUE;
    private String cua = GlobalConstants.DEFAULT_VALUE;
    private String cut = GlobalConstants.DEFAULT_VALUE;
    private String pt = GlobalConstants.DEFAULT_VALUE;
    private String ca = GlobalConstants.DEFAULT_VALUE;
    private String ac = GlobalConstants.DEFAULT_VALUE;
    private String kv_ = GlobalConstants.DEFAULT_VALUE;
    private String du = GlobalConstants.DEFAULT_VALUE;
    private String browserName = GlobalConstants.DEFAULT_VALUE;
    private String browserVersion = GlobalConstants.DEFAULT_VALUE;
    private String osName = GlobalConstants.DEFAULT_VALUE;
    private String osVersion = GlobalConstants.DEFAULT_VALUE;
    private String country = GlobalConstants.DEFAULT_VALUE;
    private String province = GlobalConstants.DEFAULT_VALUE;
    private String city = GlobalConstants.DEFAULT_VALUE;
    private static final String fieldSeparator = LogConstants.DEFAULT_FIELD_SEPARATOR;



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ver);
        dataOutput.writeUTF(s_time);
        dataOutput.writeUTF(en);
        dataOutput.writeUTF(u_ud);
        dataOutput.writeUTF(u_mid);
        dataOutput.writeUTF(u_sd);
        dataOutput.writeUTF(c_time);
        dataOutput.writeUTF(l);
        dataOutput.writeUTF(b_iev);
        dataOutput.writeUTF(b_rst);
        dataOutput.writeUTF(p_url);
        dataOutput.writeUTF(p_ref);
        dataOutput.writeUTF(tt);
        dataOutput.writeUTF(pl);
        dataOutput.writeUTF(ip);
        dataOutput.writeUTF(oid);
        dataOutput.writeUTF(on);
        dataOutput.writeUTF(cua);
        dataOutput.writeUTF(cut);
        dataOutput.writeUTF(pt);
        dataOutput.writeUTF(ca);
        dataOutput.writeUTF(ac);
        dataOutput.writeUTF(kv_);
        dataOutput.writeUTF(du);
        dataOutput.writeUTF(browserName);
        dataOutput.writeUTF(browserVersion);
        dataOutput.writeUTF(osName);
        dataOutput.writeUTF(osVersion);
        dataOutput.writeUTF(country);
        dataOutput.writeUTF(province);
        dataOutput.writeUTF(city);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.ver= dataInput.readUTF();
        this.s_time= dataInput.readUTF();
        this.en= dataInput.readUTF();
        this.u_ud= dataInput.readUTF();
        this.u_mid= dataInput.readUTF();
        this.u_sd= dataInput.readUTF();
        this.c_time= dataInput.readUTF();
        this.l= dataInput.readUTF();
        this.b_iev= dataInput.readUTF();
        this.b_rst= dataInput.readUTF();
        this.p_url= dataInput.readUTF();
        this.p_ref= dataInput.readUTF();
        this.tt= dataInput.readUTF();
        this.pl= dataInput.readUTF();
        this.ip= dataInput.readUTF();
        this.oid= dataInput.readUTF();
        this.on= dataInput.readUTF();
        this.cua= dataInput.readUTF();
        this.cut= dataInput.readUTF();
        this.pt= dataInput.readUTF();
        this.ca= dataInput.readUTF();
        this.ac= dataInput.readUTF();
        this.kv_= dataInput.readUTF();
        this.du= dataInput.readUTF();
        this.browserName= dataInput.readUTF();
        this.browserVersion= dataInput.readUTF();
        this.osName= dataInput.readUTF();
        this.osVersion= dataInput.readUTF();
        this.country= dataInput.readUTF();
        this.province= dataInput.readUTF();
        this.city= dataInput.readUTF();
    }


    public LogWritable(){
    }

    public LogWritable(String ver, String s_time, String en, String u_ud, String u_mid,
                       String u_sd, String c_time, String l, String b_iev, String b_rst,
                       String p_url, String p_ref, String tt, String pl, String ip,
                       String oid, String on, String cua, String cut, String pt,
                       String ca, String ac, String kv_, String du, String browserName,
                       String browserVersion, String osName, String osVersion, String country,
                       String province, String city) {
        this.ver = ver;
        this.s_time = s_time;
        this.en = en;
        this.u_ud = u_ud;
        this.u_mid = u_mid;
        this.u_sd = u_sd;
        this.c_time = c_time;
        this.l = l;
        this.b_iev = b_iev;
        this.b_rst = b_rst;
        this.p_url = p_url;
        this.p_ref = p_ref;
        this.tt = tt;
        this.pl = pl;
        this.ip = ip;
        this.oid = oid;
        this.on = on;
        this.cua = cua;
        this.cut = cut;
        this.pt = pt;
        this.ca = ca;
        this.ac = ac;
        this.kv_ = kv_;
        this.du = du;
        this.browserName = browserName;
        this.browserVersion = browserVersion;
        this.osName = osName;
        this.osVersion = osVersion;
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
            this.ver = ver;
    }

    public String getS_time() {
        return s_time;
    }

    public void setS_time(String s_time) {
        this.s_time = s_time;
    }

    public String getEn() {
        return en;
    }

    public void setEn(String en) {
        this.en = en;
    }

    public String getU_ud() {
        return u_ud;
    }

    public void setU_ud(String u_ud) {
        this.u_ud = u_ud;
    }

    public String getU_mid() {
        return u_mid;
    }

    public void setU_mid(String u_mid) {
        this.u_mid = u_mid;
    }

    public String getU_sd() {
        return u_sd;
    }

    public void setU_sd(String u_sd) {
        this.u_sd = u_sd;
    }

    public String getC_time() {
        return c_time;
    }

    public void setC_time(String c_time) {
        this.c_time = c_time;
    }

    public String getL() {
        return l;
    }

    public void setL(String l) {
        this.l = l;
    }

    public String getB_iev() {
        return b_iev;
    }

    public void setB_iev(String b_iev) {
        this.b_iev = b_iev;
    }

    public String getB_rst() {
        return b_rst;
    }

    public void setB_rst(String b_rst) {
        this.b_rst = b_rst;
    }

    public String getP_url() {
        return p_url;
    }

    public void setP_url(String p_url) {
        this.p_url = p_url;
    }

    public String getP_ref() {
        return p_ref;
    }

    public void setP_ref(String p_ref) {
        this.p_ref = p_ref;
    }

    public String getTt() {
        return tt;
    }

    public void setTt(String tt) {
        this.tt = tt;
    }

    public String getPl() {
        return pl;
    }

    public void setPl(String pl) {
        this.pl = pl;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getOn() {
        return on;
    }

    public void setOn(String on) {
        this.on = on;
    }

    public String getCua() {
        return cua;
    }

    public void setCua(String cua) {
        this.cua = cua;
    }

    public String getCut() {
        return cut;
    }

    public void setCut(String cut) {
        this.cut = cut;
    }

    public String getPt() {
        return pt;
    }

    public void setPt(String pt) {
        this.pt = pt;
    }

    public String getCa() {
        return ca;
    }

    public void setCa(String ca) {
        this.ca = ca;
    }

    public String getAc() {
        return ac;
    }

    public void setAc(String ac) {
        this.ac = ac;
    }

    public String getKv_() {
        return kv_;
    }

    public void setKv_(String kv_) {
        this.kv_ = kv_;
    }

    public String getDu() {
        return du;
    }

    public void setDu(String du) {
        this.du = du;
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

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    /**
     * [0] ver [1] s_time [2] en [3] u_ud [4] u_mid [5] u_sd [6] c_time [7] l [8] b_iev [9] b_rst [10] p_url
     * [11] p_ref [12] tt [13] pl [14] ip [15] oid [16] on [17] cua [18] cut [19] pt [20] ca
     * [21] ac [22] kv_ [23] du [24] browserName [25] browserVersion [26] osName [27] osVersion
     * [28] country [29] province [30] city
     */

    @Override
    public String toString() {
        return  ver+fieldSeparator+s_time+fieldSeparator+en+fieldSeparator+u_ud+fieldSeparator+
                u_mid+fieldSeparator+u_sd+fieldSeparator+c_time+fieldSeparator+l+fieldSeparator+
                b_iev+fieldSeparator+b_rst+fieldSeparator+p_url+fieldSeparator+p_ref+fieldSeparator+
                tt+fieldSeparator+pl+fieldSeparator+ip+fieldSeparator+oid+fieldSeparator+on+fieldSeparator+
                cua+fieldSeparator+cut+fieldSeparator+pt+fieldSeparator+ca+fieldSeparator+ac+fieldSeparator+
                kv_+fieldSeparator+du+fieldSeparator+browserName+fieldSeparator+browserVersion+fieldSeparator+
                osName+fieldSeparator+osVersion+fieldSeparator+country+fieldSeparator+province+fieldSeparator+city;
    }

    @Override
    public EventOutput buildInstance(LogWritable logInfo) {
        return logInfo;
    }

    @Override
    public EventOutput buildInstance(String info) {
        String[] infos = info.split(LogConstants.DEFAULT_FIELD_SEPARATOR);
        return new LogWritable(infos[0],infos[1],infos[2],infos[3],infos[4],
                infos[5],infos[6],infos[7],infos[8],infos[9],
                infos[10],infos[11],infos[12],infos[13],infos[14],
                infos[15],infos[16],infos[17],infos[18],infos[19],
                infos[20],infos[21],infos[22],infos[23],infos[24],
                infos[25],infos[26],infos[27],infos[28],infos[29],
                infos[30]);
    }
}